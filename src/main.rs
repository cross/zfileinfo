/// Represents the current state of the zdb output parser
#[derive(Debug, PartialEq)]
enum ParserState {
    Dataset,         // Initial state, expecting dataset information line
    ObjectHeader,    // Object information header state (after first blank line)
    KeyValuePairs,   // State for parsing key-value pairs (starting from dnode flags)
    BlockInfo,       // After "Indirect blocks:", parsing block information
    Other,          // After a blank line, remaining content
}

/// nb, offset is technically 63 bits, with the top bit used for a GRID or GANG block indicator.
#[derive(Debug)]
struct DVAInfo {
    vdev: u32,      // vdev id (first part of DVA, e.g. 1)
    offset: u64,    // offset (second part of DVA, e.g. 167eb4000)
    size: u64,      // size of the block (third part of DVA, e.g. 2000) (XXX what's the unit here?)
}

impl DVAInfo {
    /// Parse a DVA string in the format "vdev:offset:size" into a DVAInfo struct
    /// Returns None if the string cannot be parsed correctly
    fn from_str(dva_str: &str) -> Option<Self> {
        let parts: Vec<&str> = dva_str.split(':').collect();
        if parts.len() != 3 {
            return None;
        }

        let vdev = parts[0].parse::<u32>().ok()?; // this might also be hex; how to know?
        // Parse offset as hex since that's how it appears in zdb output
        let offset = u64::from_str_radix(parts[1], 16).ok()?;
        let size = u64::from_str_radix(parts[2], 16).ok()?;

        Some(DVAInfo {
            vdev,
            offset,
            size,
        })
    }
}

/// Represents a block info/pointer line from zdb output, e.g.:
///    0 L2   1:167eb4000:2000 20000L/c00P F=44771 B=78/78 cksum=14a59e34a60:1ad4d903aff01:191e38ff2cb6471:28b1e6c089abd03d
/// NOTE:
/// offset is tehnically
#[derive(Debug)]
struct BlockInfo {
    offset: u64,            // Logical offset (hex without prefix, e.g. 0 or 20000 not 0x...)
    level: u32,             // Block level (e.g. L2 -> 2)
    dva: DVAInfo,           // DVA information
    lsize: Option<u64>,     // Logical size (number before 'L' in PSIZE/LSIZE, e.g. 20000)
    psize: Option<u64>,     // Physical size (number before 'P' in PSIZE/LSIZE, e.g. c00 or aa00 or 20000)
    fill_count: Option<u32>,// F= value (fill count, e.g. 44771)
    birth_time: Option<String>,// B= value (birth time, e.g. 78/78)
    checksum: Option<String>, // cksum value (e.g. 14a59e34a60:...)
}
use std::process::{Command, Stdio};
use std::io::{self, BufRead, BufReader};
use std::collections::HashMap;
use clap::{Command as ClapCommand, Arg};
use std::path::Path;
use std::fs;

const VERSION: &str = "0.1.0";
const ABOUT: &str = "Analyze ZFS block information from zdb output";

/// Get the inode number for a file using ls -i
fn get_inode(path: &Path, debug: u8) -> io::Result<u64> {
    let mut cmd = Command::new("ls");
    cmd.arg("-i").arg(path);
    let output = run_command(&mut cmd, debug)?;
    
    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    
    let output_str = String::from_utf8_lossy(&output.stdout);
    // ls -i output format: "inode_number filename"
    let inode_str = output_str.split_whitespace().next()
    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Could not parse inode number"))?;
    
    inode_str.parse::<u64>()
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Invalid inode number: {}", e)))
}

/// Get the ZFS dataset name for a file using df -T
fn get_zfs_dataset(path: &Path, debug: u8) -> io::Result<String> {
    let mut cmd = Command::new("df");
    cmd.arg("-T").arg(path);
    let output = run_command(&mut cmd, debug)?;
    
    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    
    let output_str = String::from_utf8_lossy(&output.stdout);
    // Skip header line and get second line
    let line = output_str.lines()
    .nth(1)
    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Could not parse df output"))?;
    
    // Split line into fields
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 2 {
        return Err(io::Error::new(io::ErrorKind::Other, "Unexpected df output format"));
    }
    
    // First field is filesystem name, second is type
    let (filesystem, fs_type) = (fields[0], fields[1]);
    
    // Verify it's a ZFS filesystem
    if fs_type != "zfs" {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("File is not on a ZFS filesystem (found {})", fs_type)
        ));
    }
    
    Ok(filesystem.to_string())
}

fn parse_args() -> clap::ArgMatches {
    ClapCommand::new("zfileinfo")
    .version(VERSION)
    .about(ABOUT)
    .arg(
        Arg::new("file")
        .help("ZFS dataset or file to analyze")
        .required(true)
        .index(1),
    )
    .arg(
        Arg::new("debug")
        .help("Enable debug output")
        .short('d')
        .long("debug")
        .action(clap::ArgAction::Count),
    )
    .get_matches()
}

/// Helper function to run a command and collect its output
fn run_command(cmd: &mut Command, debug: u8) -> io::Result<std::process::Output> {
    if debug > 0 {
        // Build the command string for display
        let cmd_str = format!(
            "{} {}",
            cmd.get_program().to_string_lossy(),
            cmd.get_args()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
        );
        eprintln!("Executing: {}", cmd_str);
    }
    cmd.output()
}

/// Helper function to spawn a command for streaming output
fn spawn_command(cmd: &mut Command, debug: u8) -> io::Result<std::process::Child> {
    if debug > 0 {
        // Build the command string for display
        let cmd_str = format!(
            "{} {}",
            cmd.get_program().to_string_lossy(),
            cmd.get_args()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
        );
        eprintln!("Spawning: {}", cmd_str);
    }
    cmd.spawn()
}

fn main() -> io::Result<()> {
    let matches = parse_args();
    let debug = matches.get_count("debug");
    
    // Get the filename from command line arguments and validate it exists
    let path = Path::new(matches.get_one::<String>("file").expect("Required argument"));
    if !path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Path does not exist: {}", path.display())
        ));
    }
    
    if debug > 0 {
        eprintln!("Processing file: {}", path.display());
    }
    
    // Convert to absolute path
    let abs_path = fs::canonicalize(path)?;
    if debug > 0 {
        eprintln!("Canonical path: {}", abs_path.display());
    }
    
    // Get the inode number and ZFS dataset
    let inode = get_inode(&abs_path, debug)?;
    if debug > 0 {
        eprintln!("Found inode: {}", inode);
    }
    
    let dataset = get_zfs_dataset(&abs_path, debug)?;
    if debug > 0 {
        eprintln!("Found ZFS dataset: {}", dataset);
    }
    
    // Run zdb command with the dataset and inode
    if debug > 0 {
        eprintln!("\nStarting ZDB analysis...");
    }
    
    let mut cmd = Command::new("zdb");
    cmd.arg("-ddddd")
    .arg(&dataset)
    .arg(inode.to_string())
    .stdout(Stdio::piped());
    
    let mut child = spawn_command(&mut cmd, debug)?;
    
    let stdout = child.stdout.take().expect("Failed to capture stdout");
    let reader = BufReader::new(stdout);
    
    let mut kv_map: HashMap<String, String> = HashMap::new();
    let mut block_infos: Vec<BlockInfo> = Vec::new();
    let mut state = ParserState::Dataset;
    
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        
        // Handle state transitions
        match state {
            ParserState::Dataset => {
                if trimmed.is_empty() {
                    state = ParserState::ObjectHeader;
                }
                if debug > 0 {
                    eprintln!("Dataset info: {}", trimmed);
                }
                continue;
            }
            ParserState::ObjectHeader => {
                if trimmed.starts_with("dnode flags:") {
                    state = ParserState::KeyValuePairs;
                } else if !trimmed.is_empty() && debug > 0{
                    eprintln!("Object header: {}", trimmed);
                }
                if !trimmed.starts_with("dnode flags:") {
                    continue;
                }
                // Fall through to KeyValuePairs processing if this is the dnode flags line
            }
            ParserState::KeyValuePairs => {
                // Only consider lines that start with whitespace
                if line.starts_with(' ') || line.starts_with('\t') {
                    let trimmed = line.trim_start();
                    // Try to split on ':' first, else split on first whitespace
                    if let Some((key, value)) = trimmed.split_once(':') {
                        kv_map.insert(key.trim().to_string(), value.trim().to_string());
                    } else {
                        // Split on first whitespace
                        let mut parts = trimmed.splitn(2, char::is_whitespace);
                        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                            if !key.is_empty() && !value.trim().is_empty() {
                                kv_map.insert(key.trim().to_string(), value.trim().to_string());
                            }
                        }
                    }
                }
                if line.starts_with("Indirect blocks") {
                    state = ParserState::BlockInfo;
                    continue;
                }
            }
            ParserState::BlockInfo => {
                // Parse block info lines
                // Example line:
                // 0x123456 L0 1:2:3 F=4 B=5678 cksum=deadbeef
                if line.is_empty() {
                    state = ParserState::Other;
                    continue;
                }

                let tokens: Vec<&str> = line.split_whitespace().collect();
                if debug > 2{
                    eprintln!("Parsing block line: {:?}", tokens);
                }
                if tokens.len() >= 5 {
                    // offset: can be hex (0x...) or decimal
                    // Offset is always hexadecimal, no '0x' prefix
                    let offset = u64::from_str_radix(tokens[0], 16).unwrap_or(0);
                    // level: L#
                    let level = tokens[1].trim_start().strip_prefix('L').and_then(|lvl| lvl.parse().ok()).unwrap_or(0);
                    // Parse DVA string using our new method
                    let dva = DVAInfo::from_str(tokens[2]).unwrap_or(DVAInfo {
                        vdev: 0,
                        offset: 0,
                        size: 0,
                    });
                    // PSIZE/LSIZE: e.g. 20000L/c00P or 20000L/aa00P
                    let mut lsize = None;
                    let mut psize = None;
                    let size_parts: Vec<&str> = tokens[3].split('/').collect();
                    if size_parts.len() == 2 {
                        // Remove trailing 'L' and 'P'
                        lsize = size_parts[0].trim_end_matches('L').parse().ok();
                        let psize_str = size_parts[1].trim_end_matches('P');
                        psize = u64::from_str_radix(psize_str, 16).ok();
                    }
                    let mut fill_count = None;
                    let mut birth_time = None;
                    let mut checksum = None;
                    for t in &tokens[4..] {
                        if let Some(f) = t.strip_prefix("F=") {
                            fill_count = f.parse().ok();
                        } else if let Some(b) = t.strip_prefix("B=") {
                            birth_time = Some(b.to_string());
                        } else if let Some(c) = t.strip_prefix("cksum=") {
                            checksum = Some(c.to_string());
                        }
                    }
                    block_infos.push(BlockInfo {
                        offset,
                        level,
                        dva,
                        lsize,
                        psize,
                        fill_count,
                        birth_time,
                        checksum,
                    });
                }
            }
            ParserState::Other => {
                // In debug mode, print non-empty lines from the remaining output
                if debug > 0 && !trimmed.is_empty() {
                    eprintln!("Additional output: {}", trimmed);
                }
            }
        }
    }

    // Print the collected key-value pairs
    println!("Collected key-value pairs before 'Indirect blocks':");
    for (key, value) in &kv_map {
        println!("{}: {}", key, value);
    }
    
    // Print the collected block info
    if block_infos.len() < 10 {
        println!("\nCollected block info after 'Indirect blocks':");
        for block in &block_infos {
            println!("{:?}", block);
        }
    } else {
        println!("\nCollected {} block info entries after 'Indirect blocks'. Use -d for details.", block_infos.len());
    }
    Ok(())

}
