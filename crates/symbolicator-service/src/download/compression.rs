use std::io::{self, Read, Seek};
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;
use tracing::{error, info};

/// Decompresses a downloaded file.
///
/// Some compression methods are implemented by spawning an external tool and can only
/// process from a named pathname, hence we need a [`NamedTempFile`] as source.
///
/// The passed [`NamedTempFile`] might be swapped with a fresh one in case decompression happens.
/// That new temp file will be created in the same directory as the original one.
pub fn maybe_decompress_file(src: &mut NamedTempFile) -> io::Result<()> {
    let mut file = src.as_file();
    file.sync_all()?;

    let metadata = file.metadata()?;
    metric!(time_raw("objects.size") = metadata.len());

    file.rewind()?;
    if metadata.len() < 4 {
        return Ok(());
    }

    let mut magic_bytes: [u8; 4] = [0, 0, 0, 0];
    file.read_exact(&mut magic_bytes)?;
    file.rewind()?;

    match magic_bytes {
        [0x28, 0xb5, 0x2f, 0xfd] => { /* zstd logic */ }
        [0x1f, 0x8b, _, _] => { /* gzip logic */ }
        [0x78, 0x01, _, _] | [0x78, 0x9c, _, _] | [0x78, 0xda, _, _] => { /* zlib logic */ }
        [0x50, 0x4b, 0x03, 0x04] => { /* zip logic */ }
        [77, 83, 67, 70] => {
            metric!(counter("compression") += 1, "type" => "cab");

            let mut dst = tempfile_in_parent(src)?;

            let tool = if cfg!(target_os = "windows") {
                "expand"
            } else {
                "cabextract"
            };

            let mut command = Command::new(tool);

            if cfg!(target_os = "windows") {
                command
                    .arg(src.path())
                    .arg(dst.path())
                    .stderr(Stdio::piped())
                    .stdout(Stdio::piped());
            } else {
                command
                    .arg("-sfqp")
                    .arg(src.path())
                    .stdout(Stdio::from(dst.reopen()?))
                    .stderr(Stdio::piped());
            }

            let output = command.output()?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            info!("Command executed: {:?}", command);
            info!("Command stdout: {}", stdout);
            info!("Command stderr: {}", stderr);

            if !output.status.success() {
                error!(
                    "Failed to decompress CAB file with '{}': {:?}, stderr: {}",
                    tool,
                    src.path(),
                    stderr
                );
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Failed to decompress CAB file with '{}': {}",
                        tool, stderr
                    ),
                ));
            }

            info!(
                "Successfully decompressed CAB file using '{}': {:?}",
                tool,
                src.path()
            );

            std::mem::swap(src, &mut dst);
        }
        _ => {
            metric!(counter("compression") += 1, "type" => "none");
            info!("File is not compressed, skipping decompression: {:?}", src.path());
        }
    }

    Ok(())
}

// Helper function to create a temporary file in the same directory as the given file.
pub fn tempfile_in_parent(file: &NamedTempFile) -> io::Result<NamedTempFile> {
    let dir = file
        .path()
        .parent()
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
    NamedTempFile::new_in(dir)
}
