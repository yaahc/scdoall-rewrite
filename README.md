# scdoall rust rewrite example

The reason for this rewrite is that the old bash script version introduced a
number of unnecessary buffering steps and a slow awk post processing script
that significantly delayed output when working with large log files.

The usage of the original bash script was to run a command on multiple nodes in
a clustered system.

for example

```bash
scdoall 'cat /var/log/random_logfile.log' | collate_scdoall > merged_files_from_all_nodes.log
```

The rust rewrite included all of these features directly into the single
binary. The equivalent of the above command is.

```bash
sca 'cat /var/log/random_logfile.log' --merge > merged_files_from_all_nodes.log
```

Leaving off the `--merged` argument is equivalent to leaving out
`| collate_scdoall` in the original command.

There are some missing functions from the common lib I use for all the scale
script rewrites but it just contains things like "print errors at the end of a
command" or "initialize the logger" so they're unimportant.
