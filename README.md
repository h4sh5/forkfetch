# forkfetch

A remote server based download accelerator based on SSH. All you need to do is specify which SSH servers to use, and forkfetch will automatically determine available disk space, and use specified chunks and threads on each server to start downloading!

Forkfetch uses the HTTP `Range` header to split up a download into chunks, then when they finish downloading on the remote server, uses `scp` to pull them down to the local machine. The logic is that VPSs' in the cloud has a higher bandwidth and has better internet connectivity overwall, so it will accelerate the download other than directly going through a home ISP.

It can also be used as a proxied download tool, since the client machine never touches the target URL directly.


<img src=forkfetch-arch.jpg>

## Setup

Each server you use just needs to have:

1. passwordless authentication setup either with key files or SSH agent, since forkfetch doesn't take passwords
2. curl installed on it

Your client machine needs to have ssh installed (and have `scp` available). Windows backslashes are currently not supported (but Powershell or WSL should work fine).

## Example usage

Example: use `server1` and `server2` to distribute download of the URL, splitting it into 10 chunks and use 5 threads per server to download chunks:

`./forkfetch.py -r server1,server2 -n10 -t5 http://debian.anexia.at/debian-cd/12.8.0/amd64/iso-dvd/debian-12.8.0-amd64-DVD-1.iso`

Merge only to filename, no download (for when something went wrong after download but you have all the chunks):
`./forkfetch.py -o ff-download-12345 -M debian-12.8.0-amd64-DVD-1.iso`

## Usage

for help: `./forkfetch.py -h`

