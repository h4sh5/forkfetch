#!/usr/bin/env python3

import getopt
import sys
import subprocess
import datetime

def eprint(s, *args, **kwargs):
	print(s, file=sys.stderr, *args, **kwargs)

def usage():
	print('''Fetchfork distributes HTTP chunks across multiple SSH remotes to accelerate download
	-h	help
	-r	SSH remotes (comma separated, spaces ignored, passed to ssh command line. Please have publickey or sshagent setup since fetchfork doesn't prompt for passwords). Either curl or wget must be installed on remote servers, and only linux is supported
	-H	HTTP header, as used in curl (can specify multiple)
	-o	output directory
	-n	chunks to split file into for download via HTTP Range header
	-v	verbose

	example: ./fetchfork.py -r server1,user@server2 -H "Cookie: Authorization 123" https://website.com/file.zip
	''')

def main():

	headers = {}
	remotes = []
	assume_diskfree_mb = 200
	outdir = None
	verbose = False

	try:
		opts, args = getopt.getopt(sys.argv[1:], "hr:H:o:v", ["help",  "remotes", "header", "output"])
	except getopt.GetoptError as err:
		# print help information and exit:
		eprint(err)  # will print something like "option -a not recognized"
		usage()
		exit()
	
	for o, a in opts:
		if o == "-v":
			verbose = True
		elif o in ("-h", "--help"):
			usage()
			exit()
		elif o in ("-H", "--header"):
			headers[o] = a
		elif o in ("-r", "--remotes"):
			remotes = a.replace(" ",'').split(',')
		elif o in ("-o", "--output"):
			outdir = a
		else:
			assert False, "unhandled option"

	url = args[0]
	eprint("getting url:",url)
	if outdir == None:
		outdir = f"ff_download_{datetime.datetime.now().isoformat()}"
	if len(remotes) == 0:
		eprint("no remotes specified")
		usage()
		exit()

	remotes_diskfree_mb = {}
	curl_remotes = set()
	wget_remotes = set()
	# initiate checks
	if verbose:
		eprint("Connecting to remotes and check requirements..")
	for r in remotes:
		hascurl, haswget = True, True
		p = subprocess.run(["ssh", r, "curl"], capture_output=True, text=True)
		out = p.stdout + p.stderr
		if "not found" in out.lower():
			hascurl = False
		else:
			curl_remotes.add(r)
		p = subprocess.run(["ssh", r, "wget"], capture_output=True, text=True)
		out = p.stdout + p.stderr
		if "not found" in out.lower():
			haswget = False
		else:
			wget_remotes.add(r)
		if (not hascurl) and (not haswget):
			eprint(f"no curl/wget on remote host {r}")
			exit()
		
		p = subprocess.run(["ssh", r, "df -h ."], capture_output=True, text=True)
		out = p.stdout + p.stderr
		if "not found" in out.lower():
			eprint(f"unable to determine disk space on {remote}, assuming {assume_diskfree}MB")
			remotes_diskfree_mb[r] = assume_diskfree_mb * 1000000
		else:
			# get second line, 3rd element since it's usually the "Avail" table
			df_avail_str = out.split("\n")[1].split()[3].strip()
			if df_avail_str.endswith("G"): #gigabytes
				remotes_diskfree_mb[r] = float(df_avail_str.split("G")[0]) * 1000000000
			if df_avail_str.endswith("M"): #megabytes
				remotes_diskfree_mb[r] = float(df_avail_str.split("M")[0]) * 1000000
			if df_avail_str.endswith("K"): #kilobytes
				remotes_diskfree_mb[r] = float(df_avail_str.split("K")[0]) * 1000
			if verbose:
				eprint(f"remote {r} has {remotes_diskfree_mb[r]} bytes of avail disk space")

	# check how big the file is
	eprint("using the first remote to check file size..")
	file_size_bytes = -1
	headers_out = ''
	if remotes[0] in curl_remotes:
		headers_out = subprocess.check_output(["ssh", r, f"curl -I '{url}'"],text=True)
	elif remotes[0] in wget_remotes:
		headers_out = subprocess.check_output(["ssh", r, f"wget -S --spider '{url}'"],text=True)
	for line in headers_out.split("\n"):
		if line.lower().startswith("content-length"):
			file_size_bytes = float(line.split(": ")[1])
	eprint(f"file is {file_size_bytes} bytes")




if __name__ == "__main__":
	main()

