//! scdoall replacement
//!
//! Run a command on multiple hosts and post process the output

#[macro_use]
extern crate tracing;

use core::fmt::Debug;
use crossbeam::channel::{Receiver, Sender};
use failure::Error;
use regex::Regex;
use std::io::{self, prelude::*, BufRead, BufReader};
use std::ops::Deref;
use std::path::Path;
use std::process::{Command, Stdio};
use structopt::{
    clap::{AppSettings, Shell},
    StructOpt,
};

#[derive(StructOpt, Debug)]
#[structopt(raw(
    global_settings = "&[AppSettings::ColoredHelp, AppSettings::VersionlessSubcommands, AppSettings::DisableHelpSubcommand]"
))]
struct Cli {
    /// Don't display a banner for each host.
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Collate the output of all commands into a single output stream
    //
    // This is intended for timestampped data. It assumes the input streams are timestampped and
    // already sorted. It accounts for multiple line log messages by timstampping un stampped
    // messages with the timestamp of the last timestampped message.
    //
    // It also inserts the ip address of the node that the output came from after the timestamp.
    #[structopt(short = "m", long = "merge")]
    merge: bool,

    /// Don't indent output.
    #[structopt(long = "no-indent")]
    no_indent: bool,

    /// Only wait this long in seconds for ssh connect
    #[structopt(long = "timeout", default_value = "5")]
    timeout: u32,

    /// Generate a completion file
    #[structopt(
        long = "generate-completions",
        raw(possible_values = "&Shell::variants()",)
    )]
    shell: Option<Shell>,

    /// Nodes to run on
    #[structopt(long = "node", raw(number_of_values = "1"))]
    nodes: Vec<Node>,

    /// Command to run on each node
    command: Vec<String>,
}

lazy_static::lazy_static! {
    static ref ARGS: Cli = Cli::from_args();
    static ref DATE: Regex = Regex::new("^....-..-.....:..:..\\.......").unwrap();
}

#[derive(Debug, Clone)]
struct Node {
    main_ip: String,
    backplane_ip: String,
}

impl std::str::FromStr for Node {
    type Err = std::convert::Infallible;

    #[tracing::instrument]
    fn from_str(s: &str) -> Result<Node, Self::Err> {
        Ok(Node {
            main_ip: s.into(),
            backplane_ip: s.into(),
        })
    }
}

#[derive(Debug)]
struct ActiveJob<T>
where
    T: Read + Debug,
{
    incoming_lines: Option<T>,
    ident: String,
}

impl<T> ActiveJob<T>
where
    T: Read + Debug,
{
    fn process_into(&mut self, send: Sender<String>) {
        if ARGS.merge {
            self.collate_into(send);
        } else {
            self.pretty_print(send);
        }
    }

    fn pretty_print(&mut self, send: Sender<String>) {
        send.send(format!(
            "Running ({}) {}",
            ARGS.command.join(" ").replace('\n', "; "),
            self.ident
        ))
        .unwrap();

        let read = BufReader::new(self.incoming_lines.take().unwrap());

        for line in read.lines() {
            let line = line.as_ref().unwrap();
            let line = format!("        {}", line);
            send.send(line).unwrap();
        }
    }

    #[tracing::instrument]
    fn collate_into(&mut self, send: Sender<String>) {
        let read = BufReader::new(self.incoming_lines.take().unwrap());

        self.pad_ident(15);

        let mut lastline: String = "0000-00-00 00:00:00.000000 fake line".into();

        for line in read.lines() {
            let line = line.as_ref().unwrap();

            let line = if DATE.is_match(&line) {
                let (part1, part2) = line.split_at(26);
                lastline.clear();
                lastline.push_str(line);
                format!("{} {}{}", part1, self.ident, part2)
            } else {
                let (part1, _) = lastline.split_at(26);
                format!("{} {} {}", part1, self.ident, line)
            };

            send.send(line).unwrap();
        }
    }

    #[tracing::instrument]
    fn pad_ident(&mut self, size: usize) {
        let padding_len = size - self.ident.len();
        for _ in 0..padding_len {
            self.ident.push(' ');
        }
    }
}

#[tracing::instrument]
fn print_completions(shell: Shell) {
    Cli::clap().gen_completions_to("sca", shell, &mut io::stdout())
}

fn write_outputs_inorder(recvs: Vec<Receiver<String>>) {
    for recv in recvs {
        for line in recv.iter() {
            println!("{}", line);
        }
    }
}

fn merge_outputs(recvs: Vec<Receiver<String>>) {
    let m = scale::merged_chan::MergedChannels::new(recvs);

    for line in m {
        println!("{}", line);
    }
}

fn spawn_jobs(nodes: &[Node]) {
    crossbeam::scope(|scope| {
        let mut recvs = vec![];
        for node in nodes {
            let (s, r) = crossbeam::channel::bounded(8096);
            let ip = node.main_ip.clone();
            let ident = node.backplane_ip.clone();
            recvs.push(r);
            let _ = scope.spawn(move |_| {
                let args = &ARGS.command;

                let cwd = std::env::current_dir().unwrap();

                let mut cmd = Command::new("ssh");
                let _ = cmd
                    .args(
                        "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -q"
                            .split_whitespace(),
                    )
                    .arg(ip)
                    .arg("cd")
                    .arg(cwd)
                    .arg(";")
                    .args(args)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped());

                debug!(?cmd);

                let mut child = cmd.spawn().unwrap();

                debug!(?child);

                let output = child.stdout.as_mut().unwrap();

                let mut job = ActiveJob {
                    incoming_lines: Some(output),
                    ident,
                };

                debug!(?job);

                job.process_into(s);
            });
        }

        if ARGS.merge {
            merge_outputs(recvs);
        } else {
            write_outputs_inorder(recvs);
        }
    })
    .unwrap();
}

#[tracing::instrument]
fn get_node_list_from_file(path: impl AsRef<Path> + Debug) -> Vec<Node> {
    let nodes_lines = std::fs::read_to_string(path).unwrap();

    let mut nodes = vec![];

    for line in nodes_lines.lines() {
        let mut fields = line.split(' ');
        let _uuid = fields.next().unwrap().to_string();
        let backplane_ip = fields.next().unwrap().to_string();
        let main_ip = fields.next().unwrap().to_string();

        nodes.push(Node {
            main_ip,
            backplane_ip,
        });
    }

    nodes
}

#[tracing::instrument]
fn get_node_list() -> Vec<Node> {
    get_node_list_from_file(panic!("redacted"))
}

#[tracing::instrument]
fn run() -> Result<(), Error> {
    let args = ARGS.deref();
    debug!(?args);

    let mut nodes = get_node_list();
    nodes.extend_from_slice(&args.nodes);

    spawn_jobs(&nodes);

    Ok(())
}

#[tracing::instrument]
fn main() {
    scale::init_script("info");

    if let Some(shell) = ARGS.shell {
        print_completions(shell);
    } else {
        scale::handle_script_result(run());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grab_nodes() {
        scale::init_script("info");

        let nodes = get_node_list_from_file(panic!("redacted"));

        info!(?nodes);

        assert_eq!(3, nodes.len());
    }
}
