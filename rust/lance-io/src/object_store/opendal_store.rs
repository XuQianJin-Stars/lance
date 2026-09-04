// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! OpenDAL [`Operator`] adapter that implements `object_store` 0.13.
//!
//! OpenDAL main's `object_store_opendal` crate targets `object_store` 0.14,
//! which cannot share types with DataFusion 54. This module therefore talks to
//! [`Operator`] directly while preserving Lance's listing-path spelling.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt, TryStreamExt, future, stream::BoxStream};
use object_store::path::Path;
use object_store::{
    Attributes, CopyMode as ObjectStoreCopyMode, CopyOptions, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore as OSObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use opendal::options::{CopyOptions as OpendalCopyOptions, ReaderOptions, StatOptions};
use opendal::raw::percent_decode_path;
use opendal::{Buffer, BytesRange, ErrorKind, Metadata, Operator, OperatorInfo, Writer};
use tokio::sync::{Mutex, oneshot};

const DEFAULT_CONCURRENT: usize = 8;

/// Adapts OpenDAL listing paths to the spelling used by the request.
///
/// OpenDAL listing builds locations with [`Path::from`], which percent-encodes
/// reserved characters. Lance builds dataset base paths with
/// [`Path::from_url_path`], so mismatched listed locations must be decoded.
/// Locations that already match the requested prefix retain their spelling to
/// preserve paths containing literal percent escapes.
#[derive(Clone)]
pub(super) struct OpendalStore {
    info: Arc<OperatorInfo>,
    inner: Operator,
}

impl OpendalStore {
    pub(super) fn new(operator: Operator) -> Self {
        Self {
            info: operator.info().into(),
            inner: operator,
        }
    }

    async fn get_opts_without_stat(
        &self,
        location: &Path,
        raw_location: &str,
        options: &GetOptions,
    ) -> object_store::Result<GetResult> {
        let reader = self
            .inner
            .reader_options(raw_location, format_reader_options(options, None))
            .await
            .map_err(|err| format_without_stat_error(err, location.as_ref()))?;

        let mut stream = match options.range.as_ref() {
            Some(GetRange::Bounded(range)) => {
                reader.into_bytes_stream(range.start..range.end).await
            }
            Some(GetRange::Offset(offset)) => reader.into_bytes_stream(*offset..).await,
            Some(GetRange::Suffix(suffix)) => {
                reader.into_bytes_stream(BytesRange::suffix(*suffix)).await
            }
            None => reader.into_bytes_stream(..).await,
        }
        .map_err(|err| format_without_stat_error(err, location.as_ref()))?;

        let metadata = stream
            .metadata()
            .await
            .map_err(|err| format_without_stat_error(err, location.as_ref()))?;
        let attributes = format_object_attributes(&metadata);
        let meta = format_object_meta(location.as_ref(), &metadata);
        let read_range = format_read_range(options.range.as_ref(), meta.size);

        if read_range.start >= read_range.end {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: read_range,
                meta,
                attributes,
            });
        }

        if matches!(
            options.range.as_ref(),
            Some(GetRange::Bounded(range)) if range.end > meta.size
        ) {
            let reader = self
                .inner
                .reader_options(
                    raw_location,
                    format_reader_options(options, Some(meta.size)),
                )
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?;
            stream = reader
                .into_bytes_stream(read_range.start..read_range.end)
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        }

        let stream = stream.map_err(|err: io::Error| object_store::Error::Generic {
            store: "IoError",
            source: Box::new(err),
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: read_range,
            meta,
            attributes,
        })
    }

    async fn get_opts_with_stat(
        &self,
        location: &Path,
        raw_location: &str,
        options: &GetOptions,
    ) -> object_store::Result<GetResult> {
        let metadata = self
            .inner
            .stat_options(raw_location, format_stat_options(options))
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let attributes = format_object_attributes(&metadata);
        let meta = format_object_meta(location.as_ref(), &metadata);

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..0,
                meta,
                attributes,
            });
        }

        let read_range = format_read_range(options.range.as_ref(), meta.size);
        if read_range.start >= read_range.end {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: read_range,
                meta,
                attributes,
            });
        }

        let reader = self
            .inner
            .reader_options(
                raw_location,
                format_reader_options(options, Some(meta.size)),
            )
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let stream = reader
            .into_bytes_stream(read_range.start..read_range.end)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?
            .map_err(|err: io::Error| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: read_range,
            meta,
            attributes,
        })
    }

    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> object_store::Result<()> {
        let mut copy_options = OpendalCopyOptions::default();
        if if_not_exists {
            copy_options.if_not_exists = true;
        }

        self.inner
            .copy_options(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
                copy_options,
            )
            .await
            .map_err(|err| {
                if if_not_exists && err.kind() == ErrorKind::AlreadyExists {
                    object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: Box::new(err),
                    }
                } else {
                    format_object_store_error(err, from.as_ref())
                }
            })?;

        Ok(())
    }
}

impl fmt::Debug for OpendalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalStore")
            .field("scheme", &self.info.scheme())
            .field("name", &self.info.name())
            .field("root", &self.info.root())
            .field("capability", &self.info.capability())
            .finish()
    }
}

impl fmt::Display for OpendalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Opendal({}, bucket={}, root={})",
            self.info.scheme(),
            self.info.name(),
            self.info.root()
        )
    }
}

#[async_trait]
impl OSObjectStore for OpendalStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let decoded_location = percent_decode_path(location.as_ref());
        let mut future_write = self
            .inner
            .write_with(&decoded_location, Buffer::from_iter(bytes));
        let opts_mode = opts.mode.clone();
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                future_write = future_write.if_not_exists(true);
            }
            PutMode::Update(update_version) => {
                let Some(etag) = update_version.e_tag else {
                    return Err(object_store::Error::NotSupported {
                        source: Box::new(opendal::Error::new(
                            ErrorKind::Unsupported,
                            "etag is required for conditional put",
                        )),
                    });
                };
                future_write = future_write.if_match(etag.as_str());
            }
        }
        let rp = future_write.await.map_err(|err| {
            match format_object_store_error(err, location.as_ref()) {
                object_store::Error::Precondition { path, source }
                    if opts_mode == PutMode::Create =>
                {
                    object_store::Error::AlreadyExists { path, source }
                }
                e => e,
            }
        })?;

        Ok(PutResult {
            e_tag: rp.etag().map(|s| s.to_string()),
            version: rp.version().map(|s| s.to_string()),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let mut options = opendal::options::WriteOptions {
            concurrent: DEFAULT_CONCURRENT,
            ..Default::default()
        };

        let mut user_metadata = HashMap::new();
        for (key, value) in opts.attributes.iter() {
            match key {
                object_store::Attribute::CacheControl => {
                    options.cache_control = Some(value.to_string());
                }
                object_store::Attribute::ContentDisposition => {
                    options.content_disposition = Some(value.to_string());
                }
                object_store::Attribute::ContentEncoding => {
                    options.content_encoding = Some(value.to_string());
                }
                object_store::Attribute::ContentLanguage => {}
                object_store::Attribute::ContentType => {
                    options.content_type = Some(value.to_string());
                }
                object_store::Attribute::Metadata(k) => {
                    user_metadata.insert(k.to_string(), value.to_string());
                }
                _ => {}
            }
        }
        if !user_metadata.is_empty() {
            options.user_metadata = Some(user_metadata);
        }

        let decoded_location = percent_decode_path(location.as_ref());
        let writer = self
            .inner
            .writer_options(&decoded_location, options)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(Box::new(OpendalMultipartUpload::new(
            writer,
            location.clone(),
        )))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let raw_location = percent_decode_path(location.as_ref());

        if options.head {
            return self
                .get_opts_with_stat(location, &raw_location, &options)
                .await;
        }

        match self
            .get_opts_without_stat(location, &raw_location, &options)
            .await
        {
            Ok(result) => Ok(result),
            Err(object_store::Error::NotSupported { .. }) => {
                self.get_opts_with_stat(location, &raw_location, &options)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let raw_location = percent_decode_path(location.as_ref());
        let reader = self
            .inner
            .reader(&raw_location)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        let location_ref: Arc<str> = Arc::from(location.as_ref());
        futures::stream::iter(ranges.iter().cloned())
            .map(|range| {
                let reader = reader.clone();
                let location_ref = location_ref.clone();
                async move {
                    reader
                        .read(range)
                        .await
                        .map(|buf| buf.to_bytes())
                        .map_err(|err| format_object_store_error(err, &location_ref))
                }
            })
            .buffered(DEFAULT_CONCURRENT)
            .try_collect()
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let this = self.clone();
        locations
            .and_then(move |location| {
                let this = this.clone();
                async move {
                    let decoded = percent_decode_path(location.as_ref());
                    this.inner
                        .delete(&decoded)
                        .await
                        .map_err(|err| format_object_store_error(err, location.as_ref()))?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let prefix = prefix.cloned();
        let this = self.clone();
        let fut = async move {
            let stream = this
                .inner
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            let stream = stream.then(move |res| {
                let prefix = prefix.clone();
                async move {
                    let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                    let meta = format_object_meta(entry.path(), entry.metadata());
                    normalize_object_meta(meta, prefix.as_ref())
                }
            });
            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.info.capability().list_with_start_after {
            let path = prefix.map_or("".into(), |x| {
                format!("{}/", percent_decode_path(x.as_ref()))
            });
            let prefix = prefix.cloned();
            let offset = offset.clone();
            let this = self.clone();
            let fut = async move {
                let stream = this
                    .inner
                    .lister_with(&path)
                    .recursive(true)
                    .start_after(offset.as_ref())
                    .await
                    .map_err(|err| format_object_store_error(err, &path))?
                    .then(move |entry| {
                        let prefix = prefix.clone();
                        let this = this.clone();
                        let path = path.clone();
                        async move {
                            let entry =
                                entry.map_err(|err| format_object_store_error(err, &path))?;
                            let (entry_path, metadata) = entry.into_parts();
                            let object_meta =
                                if metadata.is_dir() || metadata.last_modified().is_some() {
                                    format_object_meta(&entry_path, &metadata)
                                } else {
                                    let metadata =
                                        this.inner.stat(&entry_path).await.map_err(|err| {
                                            format_object_store_error(err, &entry_path)
                                        })?;
                                    format_object_meta(&entry_path, &metadata)
                                };
                            normalize_object_meta(object_meta, prefix.as_ref())
                        }
                    });
                Ok::<_, object_store::Error>(stream)
            };
            fut.into_stream().try_flatten().boxed()
        } else {
            let offset = offset.clone();
            self.list(prefix)
                .try_filter(move |meta| future::ready(meta.location > offset))
                .boxed()
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let mut stream = self
            .inner
            .lister_with(&path)
            .await
            .map_err(|err| format_object_store_error(err, &path))?;

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().await {
            let entry = res.map_err(|err| format_object_store_error(err, ""))?;
            let meta = entry.metadata();

            if meta.is_dir() {
                common_prefixes.push(normalize_location(&entry.path().into(), prefix)?);
            } else if meta.last_modified().is_some() {
                objects.push(normalize_object_meta(
                    format_object_meta(entry.path(), meta),
                    prefix,
                )?);
            } else {
                let meta = self
                    .inner
                    .stat(entry.path())
                    .await
                    .map_err(|err| format_object_store_error(err, entry.path()))?;
                objects.push(normalize_object_meta(
                    format_object_meta(entry.path(), &meta),
                    prefix,
                )?);
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let if_not_exists = matches!(options.mode, ObjectStoreCopyMode::Create);
        self.copy_request(from, to, if_not_exists).await
    }
}

struct OpendalMultipartUpload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next_notify: oneshot::Receiver<()>,
}

impl OpendalMultipartUpload {
    fn new(writer: Writer, location: Path) -> Self {
        let (_, rx) = oneshot::channel();
        Self {
            writer: Arc::new(Mutex::new(writer)),
            location,
            next_notify: rx,
        }
    }
}

#[async_trait]
impl MultipartUpload for OpendalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        let location = self.location.clone();
        let (tx, rx) = oneshot::channel();
        let last_rx = std::mem::replace(&mut self.next_notify, rx);

        async move {
            let _ = last_rx.await;
            let mut writer = writer.lock().await;
            let result = writer
                .write(Buffer::from_iter(data))
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()));
            drop(tx);
            result
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        let metadata = writer
            .close()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))?;

        Ok(PutResult {
            e_tag: metadata.etag().map(|s| s.to_string()),
            version: metadata.version().map(|s| s.to_string()),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .abort()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))
    }
}

impl fmt::Debug for OpendalMultipartUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalMultipartUpload")
            .field("location", &self.location)
            .finish()
    }
}

fn format_object_store_error(err: opendal::Error, path: &str) -> object_store::Error {
    match err.kind() {
        ErrorKind::NotFound => object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        ErrorKind::Unsupported => object_store::Error::NotSupported {
            source: Box::new(err),
        },
        ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(err),
        },
        ErrorKind::ConditionNotMatch => object_store::Error::Precondition {
            path: path.to_string(),
            source: Box::new(err),
        },
        kind => object_store::Error::Generic {
            store: kind.into_static(),
            source: Box::new(err),
        },
    }
}

fn format_object_meta(path: &str, meta: &Metadata) -> ObjectMeta {
    ObjectMeta {
        location: path.into(),
        last_modified: meta
            .last_modified()
            .and_then(timestamp_to_datetime)
            .unwrap_or_default(),
        size: meta.content_length(),
        e_tag: meta.etag().map(|value| value.to_string()),
        version: meta.version().map(|value| value.to_string()),
    }
}

fn format_object_attributes(meta: &Metadata) -> Attributes {
    let mut attributes = Attributes::new();
    if let Some(user_meta) = meta.user_metadata() {
        for (key, value) in user_meta {
            attributes.insert(
                object_store::Attribute::Metadata(key.to_string().into()),
                value.to_string().into(),
            );
        }
    }
    attributes
}

fn format_reader_options(options: &GetOptions, content_length_hint: Option<u64>) -> ReaderOptions {
    ReaderOptions {
        version: options.version.clone(),
        if_match: options.if_match.clone(),
        if_none_match: options.if_none_match.clone(),
        if_modified_since: options.if_modified_since.and_then(datetime_to_timestamp),
        if_unmodified_since: options.if_unmodified_since.and_then(datetime_to_timestamp),
        content_length_hint,
        ..Default::default()
    }
}

fn format_stat_options(options: &GetOptions) -> StatOptions {
    StatOptions {
        version: options.version.clone(),
        if_match: options.if_match.clone(),
        if_none_match: options.if_none_match.clone(),
        if_modified_since: options.if_modified_since.and_then(datetime_to_timestamp),
        if_unmodified_since: options.if_unmodified_since.and_then(datetime_to_timestamp),
        ..Default::default()
    }
}

fn format_read_range(range: Option<&GetRange>, size: u64) -> Range<u64> {
    match range {
        Some(GetRange::Bounded(r)) => {
            if r.start >= r.end || r.start >= size {
                0..0
            } else {
                r.start..r.end.min(size)
            }
        }
        Some(GetRange::Offset(offset)) => {
            if *offset < size {
                *offset..size
            } else {
                0..0
            }
        }
        Some(GetRange::Suffix(suffix)) if *suffix < size => (size - *suffix)..size,
        _ => 0..size,
    }
}

fn format_without_stat_error(err: opendal::Error, path: &str) -> object_store::Error {
    match err.kind() {
        ErrorKind::Unsupported | ErrorKind::RangeNotSatisfied => {
            object_store::Error::NotSupported {
                source: Box::new(err),
            }
        }
        _ => format_object_store_error(err, path),
    }
}

fn timestamp_to_datetime(ts: opendal::raw::Timestamp) -> Option<DateTime<Utc>> {
    let jiff_ts = ts.into_inner();
    DateTime::<Utc>::from_timestamp(jiff_ts.as_second(), jiff_ts.subsec_nanosecond() as u32)
}

fn datetime_to_timestamp(dt: DateTime<Utc>) -> Option<opendal::raw::Timestamp> {
    opendal::raw::Timestamp::new(dt.timestamp(), dt.timestamp_subsec_nanos() as i32).ok()
}

fn normalize_location(location: &Path, prefix: Option<&Path>) -> object_store::Result<Path> {
    if prefix.is_none_or(|prefix| location.prefix_matches(prefix)) {
        return Ok(location.clone());
    }

    Path::from_url_path(location.as_ref()).map_err(|source| object_store::Error::Generic {
        store: "OpendalStore",
        source: Box::new(source),
    })
}

fn normalize_object_meta(
    mut meta: ObjectMeta,
    prefix: Option<&Path>,
) -> object_store::Result<ObjectMeta> {
    meta.location = normalize_location(&meta.location, prefix)?;
    Ok(meta)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    use opendal::services::Memory;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::raw_reserved_character("tables/run~1/t.lance")]
    #[case::literal_percent_escape("tables/run%25231/t.lance")]
    #[tokio::test]
    async fn test_list_preserves_request_path_spelling(#[case] base_url: &str) {
        let operator = Operator::new(Memory::default()).unwrap();
        let store = OpendalStore::new(operator);
        let base = Path::from_url_path(base_url).unwrap();
        let direct_location = base.clone().join("manifest.lance");
        let nested_location = Path::from_url_path(format!("{base_url}/data/part.lance")).unwrap();
        for location in [&direct_location, &nested_location] {
            store
                .put(location, Bytes::from_static(b"data").into())
                .await
                .unwrap();
        }

        let listed = store
            .list(Some(&base))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut listed_locations = listed
            .into_iter()
            .map(|meta| meta.location)
            .collect::<Vec<_>>();
        listed_locations.sort();
        let mut expected_locations = vec![direct_location.clone(), nested_location.clone()];
        expected_locations.sort();
        assert_eq!(listed_locations, expected_locations);
        assert!(
            listed_locations
                .iter()
                .all(|location| location.prefix_matches(&base))
        );

        let listed_after_nested = store
            .list_with_offset(Some(&base), &nested_location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed_after_nested.len(), 1);
        assert_eq!(listed_after_nested[0].location, direct_location);

        let delimited = store.list_with_delimiter(Some(&base)).await.unwrap();
        assert_eq!(delimited.objects.len(), 1);
        assert_eq!(delimited.objects[0].location, direct_location);
        assert_eq!(delimited.common_prefixes, vec![base.clone().join("data")]);
    }
}
