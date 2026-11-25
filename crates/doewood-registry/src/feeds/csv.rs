//! A small bounded CSV reader (RFC 4180: quoted fields, `""` escapes, line breaks inside
//! quotes) that keeps only the columns asked for. A record longer than the cap is an error,
//! never buffered whole.

use std::io::BufRead;

/// Why a record could not be read.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CsvError {
    /// A record longer than the cap.
    TooLong,
    /// Reading failed (or the text is not UTF-8 in a kept column).
    Io(String),
}

/// Reads records from `input`, each at most `max_record` bytes.
pub(crate) struct Reader<R> {
    input: R,
    max_record: usize,
}

impl<R: BufRead> Reader<R> {
    pub(crate) fn new(input: R, max_record: usize) -> Self {
        Self { input, max_record }
    }

    /// The next record's fields at `keep` (indexes; a missing one is empty), or `None` at the
    /// end. With `keep` empty, every field is returned (for the header).
    pub(crate) fn next(&mut self, keep: &[usize]) -> Result<Option<Vec<String>>, CsvError> {
        let mut fields: Vec<String> = Vec::new();
        let mut field: Vec<u8> = Vec::new();
        let mut index = 0usize;
        let mut quoted = false;
        // The previous byte was a quote inside a quoted field (an escape or the end).
        let mut quote = false;
        let mut started = false;
        let mut len = 0usize;
        let keep_field = |fields: &mut Vec<String>, field: &mut Vec<u8>, index: usize| {
            if keep.is_empty() || keep.contains(&index) {
                fields.push(String::from_utf8_lossy(field).into_owned());
            }
            field.clear();
        };
        loop {
            let buf = self
                .input
                .fill_buf()
                .map_err(|e| CsvError::Io(e.to_string()))?;
            if buf.is_empty() {
                if !started {
                    return Ok(None);
                }
                keep_field(&mut fields, &mut field, index);
                return Ok(Some(order(fields, keep)));
            }
            let mut used = 0;
            let mut done = false;
            for &b in buf {
                used += 1;
                len += 1;
                if len > self.max_record {
                    return Err(CsvError::TooLong);
                }
                started = true;
                if quoted {
                    if quote {
                        quote = false;
                        if b == b'"' {
                            field.push(b'"');
                            continue;
                        }
                        quoted = false;
                    } else if b == b'"' {
                        quote = true;
                        continue;
                    } else {
                        field.push(b);
                        continue;
                    }
                }
                match b {
                    b'"' if field.is_empty() => quoted = true,
                    b',' => {
                        keep_field(&mut fields, &mut field, index);
                        index += 1;
                    }
                    b'\n' => {
                        if field.last() == Some(&b'\r') {
                            field.pop();
                        }
                        keep_field(&mut fields, &mut field, index);
                        done = true;
                        break;
                    }
                    _ => field.push(b),
                }
            }
            self.input.consume(used);
            if done {
                return Ok(Some(order(fields, keep)));
            }
        }
    }
}

/// `fields` (in column order) as asked in `keep`, missing ones empty.
fn order(fields: Vec<String>, keep: &[usize]) -> Vec<String> {
    if keep.is_empty() {
        return fields;
    }
    let mut sorted: Vec<usize> = keep.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let by_col: Vec<(usize, String)> = sorted.into_iter().zip(fields).collect();
    keep.iter()
        .map(|k| {
            by_col
                .iter()
                .find(|(c, _)| c == k)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(text: &str, keep: &[usize], max: usize) -> Result<Vec<Vec<String>>, CsvError> {
        let mut r = Reader::new(text.as_bytes(), max);
        let mut out = Vec::new();
        while let Some(rec) = r.next(keep)? {
            out.push(rec);
        }
        Ok(out)
    }

    #[test]
    fn quoted_fields_escapes_and_line_breaks() {
        let text = "\"LEI\",\"Entity.LegalName\",\"X\"\r\n\
                    \"A1\",\"Acme, \"\"Test\"\" Ltd\",\"x\"\n\
                    B2,\"two\nlines\",y\n\
                    C3,,\n\
                    D4";
        assert_eq!(
            all(text, &[], 1000).unwrap()[0],
            ["LEI", "Entity.LegalName", "X"]
        );
        assert_eq!(
            all(text, &[0, 1], 1000).unwrap()[1..],
            [
                vec!["A1".to_owned(), "Acme, \"Test\" Ltd".to_owned()],
                vec!["B2".to_owned(), "two\nlines".to_owned()],
                vec!["C3".to_owned(), String::new()],
                vec!["D4".to_owned(), String::new()],
            ]
        );
        // Columns come back in the order asked.
        assert_eq!(all("a,b,c\n", &[2, 0], 100).unwrap(), [["c", "a"]]);
    }

    #[test]
    fn records_are_capped() {
        let long = format!("a,{}\nb,c\n", "x".repeat(100));
        assert_eq!(all(&long, &[0], 50), Err(CsvError::TooLong));
        assert_eq!(all("", &[0], 50).unwrap(), Vec::<Vec<String>>::new());
    }
}
