use apalis_core::task::metadata::MetadataStore;
use lapin::{
    protocol::basic::AMQPProperties,
    types::{AMQPValue, FieldTable},
};
use std::collections::HashMap;
/// Prefix used for headers that map to/from apalis metadata keys.
/// Only headers with this prefix are flattened into the MetadataStore;
/// other headers are left alone (not represented in MetadataStore).
const HEADER_PREFIX: &str = "x-apalis-";

/// Metadata keys for each non-header AMQPProperties field, so every
/// property round-trips through the same MetadataStore.
mod keys {
    pub(crate) const CONTENT_TYPE: &str = "content_type";
    pub(crate) const CONTENT_ENCODING: &str = "content_encoding";
    pub(crate) const DELIVERY_MODE: &str = "delivery_mode";
    pub(crate) const PRIORITY: &str = "priority";
    pub(crate) const CORRELATION_ID: &str = "correlation_id";
    pub(crate) const REPLY_TO: &str = "reply_to";
    pub(crate) const EXPIRATION: &str = "expiration";
    pub(crate) const MESSAGE_ID: &str = "message_id";
    pub(crate) const TIMESTAMP: &str = "timestamp";
    pub(crate) const KIND: &str = "kind";
    pub(crate) const USER_ID: &str = "user_id";
    pub(crate) const APP_ID: &str = "app_id";
    pub(crate) const CLUSTER_ID: &str = "cluster_id";
}

/// Converts `AMQPProperties` into a `MetadataStore`. Every present
/// (non-`None`) field is stored under its own key, stringified.
/// `headers` entries prefixed with `x-apalis-` are flattened into the
/// store under their unprefixed key; other headers are dropped (they
/// aren't apalis-owned, so they have no place in MetadataStore).
pub(crate) fn properties_to_metadata(props: &AMQPProperties) -> MetadataStore {
    let mut map = HashMap::new();

    macro_rules! put {
        ($accessor:ident, $key:expr) => {
            if let Some(v) = props.$accessor() {
                map.insert($key.to_string(), v.to_string());
            }
        };
    }

    put!(content_type, keys::CONTENT_TYPE);
    put!(content_encoding, keys::CONTENT_ENCODING);
    put!(correlation_id, keys::CORRELATION_ID);
    put!(reply_to, keys::REPLY_TO);
    put!(expiration, keys::EXPIRATION);
    put!(message_id, keys::MESSAGE_ID);
    put!(kind, keys::KIND);
    put!(user_id, keys::USER_ID);
    put!(app_id, keys::APP_ID);
    put!(cluster_id, keys::CLUSTER_ID);
    put!(delivery_mode, keys::DELIVERY_MODE);
    put!(priority, keys::PRIORITY);
    put!(timestamp, keys::TIMESTAMP);

    if let Some(headers) = props.headers() {
        for (key, value) in headers.inner() {
            if let Some(stripped) = key.as_str().strip_prefix(HEADER_PREFIX) {
                if let Some(s) = amqp_value_as_string(value) {
                    map.insert(stripped.to_string(), s);
                }
            }
        }
    }

    MetadataStore::from_map(map)
}

/// Rebuilds `AMQPProperties` from a `MetadataStore`. Recognized keys
/// (see the `keys` module) are placed back into their dedicated
/// fields; every other key is re-prefixed with `x-apalis-` and stored
/// in `headers`, so a round trip through `properties_to_metadata` and
/// back is lossless for apalis-owned data.
pub(crate) fn metadata_to_properties(metadata: &MetadataStore) -> AMQPProperties {
    let mut props = AMQPProperties::default();
    let mut headers = FieldTable::default();

    for (key, value) in metadata.iter() {
        match key.as_str() {
            keys::CONTENT_TYPE => {
                props = props.with_content_type(value.as_str().into());
            }
            keys::CONTENT_ENCODING => {
                props = props.with_content_encoding(value.as_str().into());
            }
            keys::CORRELATION_ID => {
                props = props.with_correlation_id(value.as_str().into());
            }
            keys::REPLY_TO => {
                props = props.with_reply_to(value.as_str().into());
            }
            keys::EXPIRATION => {
                props = props.with_expiration(value.as_str().into());
            }
            keys::MESSAGE_ID => {
                props = props.with_message_id(value.as_str().into());
            }
            keys::KIND => {
                props = props.with_type(value.as_str().into());
            }
            keys::USER_ID => {
                props = props.with_user_id(value.as_str().into());
            }
            keys::APP_ID => {
                props = props.with_app_id(value.as_str().into());
            }
            keys::CLUSTER_ID => {
                props = props.with_cluster_id(value.as_str().into());
            }
            keys::DELIVERY_MODE => {
                if let Ok(v) = value.parse() {
                    props = props.with_delivery_mode(v);
                }
            }
            keys::PRIORITY => {
                if let Ok(v) = value.parse() {
                    props = props.with_priority(v);
                }
            }
            keys::TIMESTAMP => {
                if let Ok(v) = value.parse() {
                    props = props.with_timestamp(v);
                }
            }
            other => {
                let header_key = format!("{HEADER_PREFIX}{other}");
                headers.insert(
                    header_key.into(),
                    AMQPValue::LongString(value.as_str().into()),
                );
            }
        }
    }

    if !headers.inner().is_empty() {
        props = props.with_headers(headers);
    }

    props
}

fn amqp_value_as_string(value: &AMQPValue) -> Option<String> {
    match value {
        AMQPValue::LongString(s) => Some(s.to_string()),
        AMQPValue::ShortString(s) => Some(s.to_string()),
        AMQPValue::LongLongInt(i) => Some(i.to_string()),
        AMQPValue::LongInt(i) => Some(i.to_string()),
        AMQPValue::Boolean(b) => Some(b.to_string()),
        _ => None,
    }
}
