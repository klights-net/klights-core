/// Convert protobuf IntOrString to JSON value
use crate::protobuf::*;
pub fn intorstring_to_json(ios: &k8s_pb::apimachinery::pkg::util::intstr::IntOrString) -> Value {
    use serde_json::json;
    // Use the type discriminant (0 = int, 1 = string) as the authoritative signal.
    // The Go client (gogoproto) may include intVal = 0 on the wire even for string-type
    // IntOrStrings, so checking int_val first would incorrectly return 0 for "2%" etc.
    if ios.r#type == Some(1) {
        if let Some(str_val) = &ios.str_val {
            return json!(str_val);
        }
        return json!("");
    }
    if let Some(int_val) = ios.int_val {
        json!(int_val)
    } else if let Some(str_val) = &ios.str_val {
        json!(str_val)
    } else {
        json!(0)
    }
}
