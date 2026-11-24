//! The content checks of a fetched icon (ADR 0002 section 5.4).
//!
//! - The format comes from the magic bytes only (never from `Content-Type`): ICO, PNG, GIF,
//!   JPEG, WEBP or AVIF. Never SVG: anything that starts like markup (`<`, after a byte-order
//!   mark and white space) is refused whatever it claims to be.
//! - At most [`MAX_ICON_BYTES`].
//! - Blank icons are refused: under 8 px wide or high, fully transparent, one flat colour, or
//!   near-white. ICO, PNG, GIF, JPEG and WEBP are decoded for this (within decode limits; ICO's
//!   largest image, GIF's first frame). AVIF has no decoder in the build: its size is read from
//!   the `ispe` property, and the pixel checks are skipped.

use std::io::Cursor;

use image::{ImageFormat, ImageReader, Limits, RgbaImage};

/// The largest icon accepted, in bytes (200 KB).
pub const MAX_ICON_BYTES: usize = 200 * 1024;
/// The smallest width and height accepted, in pixels.
pub const MIN_SIDE: u32 = 8;
/// The largest width and height decoded, in pixels.
pub const MAX_SIDE: u32 = 2048;

/// An accepted icon format, from the magic bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IconFormat {
    /// Windows icon (`00 00 01 00`).
    Ico,
    /// PNG.
    Png,
    /// GIF (87a or 89a).
    Gif,
    /// JPEG.
    Jpeg,
    /// WEBP (RIFF).
    Webp,
    /// AVIF (ISO BMFF with an `avif` or `avis` brand).
    Avif,
}

impl IconFormat {
    /// The `Content-Type` doewood serves it with.
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Ico => "image/x-icon",
            Self::Png => "image/png",
            Self::Gif => "image/gif",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
            Self::Avif => "image/avif",
        }
    }

    fn image_format(self) -> Option<ImageFormat> {
        match self {
            Self::Ico => Some(ImageFormat::Ico),
            Self::Png => Some(ImageFormat::Png),
            Self::Gif => Some(ImageFormat::Gif),
            Self::Jpeg => Some(ImageFormat::Jpeg),
            Self::Webp => Some(ImageFormat::WebP),
            Self::Avif => None,
        }
    }
}

/// Why an icon was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    /// Empty.
    #[error("empty")]
    Empty,
    /// Over [`MAX_ICON_BYTES`].
    #[error("larger than {MAX_ICON_BYTES} bytes")]
    TooLarge,
    /// The bytes look like markup (SVG, HTML, XML).
    #[error("markup (SVG, HTML or XML), not an image")]
    Markup,
    /// `Content-Type` says SVG.
    #[error("SVG is never accepted")]
    Svg,
    /// Not one of the accepted formats.
    #[error("not ICO, PNG, GIF, JPEG, WEBP or AVIF")]
    Format,
    /// The image does not decode (or exceeds the decode limits).
    #[error("does not decode")]
    Corrupt,
    /// Under [`MIN_SIDE`] pixels wide or high.
    #[error("smaller than {MIN_SIDE} px")]
    TooSmall,
    /// Fully transparent.
    #[error("fully transparent")]
    Transparent,
    /// One flat colour.
    #[error("one flat colour")]
    Flat,
    /// Near-white.
    #[error("near-white")]
    NearWhite,
}

/// Whether the bytes start like markup: optional UTF-8 BOM and white space, then `<`. Also
/// UTF-16 and UTF-32 byte-order marks (no image starts with one).
pub fn looks_like_markup(bytes: &[u8]) -> bool {
    if bytes.starts_with(&[0xFF, 0xFE])
        || bytes.starts_with(&[0xFE, 0xFF])
        || bytes.starts_with(&[0, 0, 0xFE, 0xFF])
    {
        return true;
    }
    let rest = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    rest.iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| *b == b'<')
}

/// The format of `bytes` by its magic bytes.
pub fn sniff(bytes: &[u8]) -> Option<IconFormat> {
    if bytes.starts_with(&[0, 0, 1, 0]) && bytes.len() >= 6 && (bytes[4] != 0 || bytes[5] != 0) {
        return Some(IconFormat::Ico);
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some(IconFormat::Png);
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some(IconFormat::Gif);
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some(IconFormat::Jpeg);
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some(IconFormat::Webp);
    }
    if is_avif(bytes) {
        return Some(IconFormat::Avif);
    }
    None
}

/// An ISO BMFF `ftyp` box first, whose major or a compatible brand is `avif` or `avis`.
fn is_avif(bytes: &[u8]) -> bool {
    if bytes.len() < 16 || &bytes[4..8] != b"ftyp" {
        return false;
    }
    let size = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if size < 16 || size > bytes.len() {
        return false;
    }
    let brands = std::iter::once(&bytes[8..12]).chain(bytes[16..size].chunks_exact(4));
    brands.into_iter().any(|b| b == b"avif" || b == b"avis")
}

/// The width and height in the first `ispe` property of an AVIF file.
fn avif_size(bytes: &[u8]) -> Option<(u32, u32)> {
    let at = bytes.windows(4).position(|w| w == b"ispe")?;
    // 'ispe', version and flags (4), width (4), height (4).
    let field = |o: usize| {
        bytes
            .get(at + o..at + o + 4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    };
    Some((field(8)?, field(12)?))
}

/// Checks an icon fetched with `content_type`: the format, the size and the pixels. Returns
/// its format, from the magic bytes.
pub fn check(bytes: &[u8], content_type: Option<&str>) -> Result<IconFormat, Rejection> {
    if bytes.is_empty() {
        return Err(Rejection::Empty);
    }
    if bytes.len() > MAX_ICON_BYTES {
        return Err(Rejection::TooLarge);
    }
    if content_type.is_some_and(|t| t.to_ascii_lowercase().contains("svg")) {
        return Err(Rejection::Svg);
    }
    if looks_like_markup(bytes) {
        return Err(Rejection::Markup);
    }
    let format = sniff(bytes).ok_or(Rejection::Format)?;
    match format.image_format() {
        None => {
            let (w, h) = avif_size(bytes).ok_or(Rejection::Corrupt)?;
            if w < MIN_SIDE || h < MIN_SIDE {
                return Err(Rejection::TooSmall);
            }
        }
        Some(f) => check_pixels(&decode(bytes, f)?)?,
    }
    Ok(format)
}

fn decode(bytes: &[u8], format: ImageFormat) -> Result<RgbaImage, Rejection> {
    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_SIDE);
    limits.max_image_height = Some(MAX_SIDE);
    limits.max_alloc = Some(64 * 1024 * 1024);
    let mut reader = ImageReader::with_format(Cursor::new(bytes), format);
    reader.limits(limits);
    // A decoder panic on a hostile file must not end the request's task.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        reader.decode().map(image::DynamicImage::into_rgba8)
    }))
    .map_err(|_| Rejection::Corrupt)?
    .map_err(|_| Rejection::Corrupt)
}

/// A pixel counts as visible from this alpha on.
const VISIBLE_ALPHA: u8 = 16;
/// Pixels within this of the first one, on every channel, are the same flat colour.
const FLAT_TOLERANCE: u8 = 6;
/// A visible pixel whose red, green and blue are all at least this is near-white.
const NEAR_WHITE: u8 = 240;

fn check_pixels(img: &RgbaImage) -> Result<(), Rejection> {
    if img.width() < MIN_SIDE || img.height() < MIN_SIDE {
        return Err(Rejection::TooSmall);
    }
    let Some(first) = img.pixels().next().map(|p| p.0) else {
        return Err(Rejection::TooSmall);
    };
    let mut visible = first[3] >= VISIBLE_ALPHA;
    let mut flat = true;
    let mut white = !visible || first[..3].iter().all(|c| *c >= NEAR_WHITE);
    for p in img.pixels().map(|p| p.0) {
        if flat
            && p.iter()
                .zip(first)
                .any(|(a, b)| a.abs_diff(b) > FLAT_TOLERANCE)
        {
            flat = false;
        }
        if p[3] >= VISIBLE_ALPHA {
            visible = true;
            if white && p[..3].iter().any(|c| *c < NEAR_WHITE) {
                white = false;
            }
        }
        if !flat && visible && !white {
            return Ok(());
        }
    }
    if !visible {
        Err(Rejection::Transparent)
    } else if flat {
        Err(Rejection::Flat)
    } else {
        Err(Rejection::NearWhite)
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
pub(crate) mod tests {
    use image::{DynamicImage, Rgba};

    use super::*;

    /// A `w` by `h` image, each pixel from `f(x, y)`, encoded as `format`.
    pub(crate) fn encode(
        w: u32,
        h: u32,
        format: ImageFormat,
        f: impl Fn(u32, u32) -> [u8; 4],
    ) -> Vec<u8> {
        let img = RgbaImage::from_fn(w, h, |x, y| Rgba(f(x, y)));
        let img = if format == ImageFormat::Jpeg {
            DynamicImage::ImageRgba8(img).into_rgb8().into()
        } else {
            DynamicImage::ImageRgba8(img)
        };
        let mut out = Cursor::new(Vec::new());
        img.write_to(&mut out, format).unwrap();
        out.into_inner()
    }

    /// A 16 by 16 icon: a dark diagonal stripe on light blue.
    pub(crate) fn logo(format: ImageFormat) -> Vec<u8> {
        encode(16, 16, format, |x, y| {
            if x.abs_diff(y) < 3 {
                [20, 30, 120, 255]
            } else {
                [180, 210, 250, 255]
            }
        })
    }

    #[test]
    fn real_icons_pass_in_every_decoded_format() {
        for (format, expected) in [
            (ImageFormat::Png, IconFormat::Png),
            (ImageFormat::Ico, IconFormat::Ico),
            (ImageFormat::Gif, IconFormat::Gif),
            (ImageFormat::Jpeg, IconFormat::Jpeg),
            (ImageFormat::WebP, IconFormat::Webp),
        ] {
            let bytes = logo(format);
            assert_eq!(sniff(&bytes), Some(expected), "{format:?}");
            assert_eq!(
                check(&bytes, Some("image/whatever")),
                Ok(expected),
                "{format:?}"
            );
            // The upstream Content-Type plays no part (unless it says SVG).
            assert_eq!(check(&bytes, Some("text/html")), Ok(expected), "{format:?}");
            assert_eq!(check(&bytes, None), Ok(expected), "{format:?}");
        }
    }

    /// A minimal AVIF header: `ftyp` with brand `avif`, then an `ispe` property.
    fn avif(w: u32, h: u32) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&20u32.to_be_bytes());
        b.extend_from_slice(b"ftypavif");
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(b"mif1");
        b.extend_from_slice(&20u32.to_be_bytes());
        b.extend_from_slice(b"ispe");
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&w.to_be_bytes());
        b.extend_from_slice(&h.to_be_bytes());
        b
    }

    #[test]
    fn avif_is_sniffed_and_its_size_checked() {
        assert_eq!(check(&avif(32, 32), None), Ok(IconFormat::Avif));
        assert_eq!(check(&avif(4, 32), None), Err(Rejection::TooSmall));
        let mut no_ispe = avif(32, 32);
        no_ispe.truncate(20);
        assert_eq!(check(&no_ispe, None), Err(Rejection::Corrupt));
        let mut heic = avif(32, 32);
        heic[8..12].copy_from_slice(b"heic");
        heic[16..20].copy_from_slice(b"heic");
        assert_eq!(check(&heic, None), Err(Rejection::Format));
    }

    #[test]
    fn svg_is_never_accepted() {
        let svg = br#"<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16"><rect width="16" height="16"/></svg>"#;
        assert_eq!(check(svg, Some("image/svg+xml")), Err(Rejection::Svg));
        // By the bytes, whatever the type says.
        assert_eq!(check(svg, Some("image/png")), Err(Rejection::Markup));
        assert_eq!(check(svg, None), Err(Rejection::Markup));
        let xml = b"\xEF\xBB\xBF  \n<?xml version=\"1.0\"?><svg/>";
        assert_eq!(check(xml, Some("image/x-icon")), Err(Rejection::Markup));
        // A real PNG served as SVG is refused too.
        assert_eq!(
            check(
                &logo(ImageFormat::Png),
                Some("Image/SVG+XML; charset=utf-8")
            ),
            Err(Rejection::Svg)
        );
        // UTF-16 markup.
        let utf16: Vec<u8> = [0xFF, 0xFE]
            .into_iter()
            .chain("<svg/>".encode_utf16().flat_map(u16::to_le_bytes))
            .collect();
        assert_eq!(check(&utf16, Some("image/png")), Err(Rejection::Markup));
    }

    #[test]
    fn html_as_png_is_refused() {
        let html = b"<!doctype html><html><body>Not found</body></html>";
        assert_eq!(check(html, Some("image/png")), Err(Rejection::Markup));
        assert_eq!(
            check(b"Not found", Some("image/png")),
            Err(Rejection::Format)
        );
        assert_eq!(check(b"", Some("image/png")), Err(Rejection::Empty));
        // A PNG signature with garbage after it.
        let mut fake = b"\x89PNG\r\n\x1a\n".to_vec();
        fake.extend_from_slice(b"<html>");
        assert_eq!(check(&fake, None), Err(Rejection::Corrupt));
        // Other images that are not accepted: BMP, TIFF.
        assert_eq!(check(b"BM\0\0\0\0\0\0\0\0", None), Err(Rejection::Format));
        assert_eq!(check(b"II*\0\x08\0\0\0", None), Err(Rejection::Format));
    }

    #[test]
    fn oversize_is_refused() {
        let mut big = logo(ImageFormat::Png);
        big.resize(MAX_ICON_BYTES + 1, 0);
        assert_eq!(check(&big, None), Err(Rejection::TooLarge));
    }

    #[test]
    fn blank_icons_are_refused() {
        let png = ImageFormat::Png;
        assert_eq!(
            check(&encode(16, 16, png, |_, _| [0, 0, 0, 0]), None),
            Err(Rejection::Transparent)
        );
        assert_eq!(
            check(
                &encode(16, 16, png, |x, _| [9, 9, 9, (x % 2) as u8 * 10]),
                None
            ),
            Err(Rejection::Transparent)
        );
        assert_eq!(
            check(&encode(16, 16, png, |_, _| [200, 0, 0, 255]), None),
            Err(Rejection::Flat)
        );
        assert_eq!(
            check(
                &encode(16, 16, png, |x, _| [100, 100, 100 + (x % 3) as u8, 255]),
                None
            ),
            Err(Rejection::Flat)
        );
        assert_eq!(
            check(
                &encode(16, 16, png, |x, y| {
                    if x == y {
                        [245, 250, 255, 255]
                    } else {
                        [255, 255, 255, 255]
                    }
                }),
                None
            ),
            Err(Rejection::NearWhite)
        );
        // White on transparent is near-white too.
        assert_eq!(
            check(
                &encode(16, 16, png, |x, _| if x < 8 { [255; 4] } else { [0; 4] }),
                None
            ),
            Err(Rejection::NearWhite)
        );
        assert_eq!(
            check(
                &encode(7, 16, png, |x, y| [x as u8 * 30, y as u8 * 10, 0, 255]),
                None
            ),
            Err(Rejection::TooSmall)
        );
        assert_eq!(
            check(
                &encode(16, 4, png, |x, y| [x as u8 * 10, y as u8 * 30, 0, 255]),
                None
            ),
            Err(Rejection::TooSmall)
        );
        // A dark glyph on transparent is a real icon.
        assert_eq!(
            check(
                &encode(16, 16, png, |x, _| if x < 8 {
                    [0, 0, 0, 255]
                } else {
                    [0; 4]
                }),
                None
            ),
            Ok(IconFormat::Png)
        );
    }

    #[test]
    fn decode_limits_hold() {
        // A PNG header claiming 100000 x 100000 pixels.
        let mut png = logo(ImageFormat::Png);
        png[16..20].copy_from_slice(&100_000u32.to_be_bytes());
        png[20..24].copy_from_slice(&100_000u32.to_be_bytes());
        assert_eq!(check(&png, None), Err(Rejection::Corrupt));
    }
}
