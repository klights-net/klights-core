/// Expand `$(VAR_NAME)` references using previously resolved environment values.
/// Undefined references remain literal, matching Kubernetes expansion behavior.
pub fn expand_env_var_references(
    value: &str,
    resolved: &std::collections::HashMap<String, String>,
) -> String {
    if !value.contains("$(") {
        return value.to_string();
    }
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'(') {
            chars.next();
            let mut var_name = String::new();
            let mut closed = false;
            for inner in chars.by_ref() {
                if inner == ')' {
                    closed = true;
                    break;
                }
                var_name.push(inner);
            }
            if closed {
                if let Some(replacement) = resolved.get(&var_name) {
                    result.push_str(replacement);
                } else {
                    result.push('$');
                    result.push('(');
                    result.push_str(&var_name);
                    result.push(')');
                }
            } else {
                result.push('$');
                result.push('(');
                result.push_str(&var_name);
            }
        } else {
            result.push(c);
        }
    }
    result
}
