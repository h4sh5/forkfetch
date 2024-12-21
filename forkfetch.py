#!/usr/bin/env python3

import getopt
import sys
import subprocess
from subprocess import PIPE
import datetime
import time
import random
import os
import glob


def eprint(s, *args, **kwargs):
	print(s, file=sys.stderr, *args, **kwargs)


def usage():
	print('''forkfetch distributes HTTP chunks across multiple SSH remotes to accelerate download
	-h	help
	-r	SSH remotes (comma separated, spaces ignored, passed to ssh command line. Please have publickey or sshagent setup since forkfetch doesn't prompt for passwords). curl must be installed on remote servers (wget support might come later)
	-H	HTTP header, as used in curl (can specify multiple)
	-o	specify alternative output directory
	-n	chunks to split file into for each remote (default 10)
	-t	threads per remote host (default 2)
	-M	merge only (no download), -o must be specified
	-v	verbose

	example: ./forkfetch.py -r server1,user@server2 -H "Cookie: Authorization 123" https://website.com/file.zip
	''')

STARTTIME = int(time.time())
start = time.time()


def get_filename_from_jobid(jobid):
	return f"ff-{jobid}"


def main():

	headers = []
	remotes = []
	assume_diskfree = 200000000 # 200MB
	chunks = 10
	threads_per_remote = 2
	outdir = None
	verbose = False
	merge_only = False
	outdir_specified = False

	try:
		opts, args = getopt.getopt(sys.argv[1:], "hr:H:o:t:n:Mv", ["help",  "remotes", "header", "output"])
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
			if "range" not in a.lower():
				headers.append(a)
			else:
				eprint("Range header cant be added, skipping")
		elif o in ("-r", "--remotes"):
			remotes = a.replace(" ",'').split(',')
		elif o in ("-o", "--output"):
			outdir = a
			outdir_specified = True
			if "_" in outdir:
				eprint("WARNING: _ replaced with - in output directory name due to sorting conventions")
		elif o in ("-n"):
			chunks = int(a)
		elif o in ("-t"):
			threads_per_remote = int(a)
		elif o in ("-M"):
			merge_only = True
		else:
			assert False, "unhandled option"

	if merge_only and not outdir_specified:
		eprint("Must specify outdir in merge only mode")
		exit()

	url = args[0]

	if not merge_only:
		eprint("getting url:",url)
		if outdir == None:
			outdir = f"ff-download-{STARTTIME}"
		if len(remotes) == 0:
			eprint("no remotes specified")
			usage()
			exit()

		os.mkdir(outdir)

		remotes_diskfree = {}
		curl_remotes = set()
		wget_remotes = set()
		# initiate checks
		eprint("start timestamp:", STARTTIME)
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
				eprint(f"unable to determine disk space on {r}, assuming {assume_diskfree/1000000} MB")
				remotes_diskfree[r] = assume_diskfree
			else:
				# get second line, 3rd element since it's usually the "Avail" table
				df_avail_str = out.split("\n")[1].split()[3].strip()
				if df_avail_str.endswith("T"): # terabytes
					remotes_diskfree[r] = int(float(df_avail_str.split("T")[0]) * 1000000000000)
				if df_avail_str.endswith("G"): #gigabytes
					remotes_diskfree[r] = int(float(df_avail_str.split("G")[0]) * 1000000000)
				if df_avail_str.endswith("M"): #megabytes
					remotes_diskfree[r] = int((float(df_avail_str.split("M")[0]) - 5) * 1000000) # save 5M so system doesn crash
				if df_avail_str.endswith("K"): #kilobytes
					remotes_diskfree[r] = int((float(df_avail_str.split("K")[0]) - 200) * 1000) # save 200K 
				if df_avail_str.endswith("B"):
					remotes_diskfree[r] = int(float(df_avail_str.split("B")[0])) # I mean, come on..
				if verbose:
					eprint(f"remote {r} has {remotes_diskfree[r]} bytes of avail disk space")

		# check how big the file is
		eprint("using the first remote to check file size..")
		file_size_bytes = -1
		headers_out = ''
		
		
		if remotes[0] in curl_remotes:
			headers_out = subprocess.check_output(["ssh", r, f"curl -I '{url}'"],text=True)
		elif remotes[0] in wget_remotes:
			headers_out = subprocess.check_output(["ssh", r, f"wget -S --spider '{url}' 2>&1"],text=True)
		for line in headers_out.split("\n"):
			if line.strip().lower().startswith("content-length"):
				file_size_bytes = float(line.split(": ")[1])
		eprint(f"file is {file_size_bytes} bytes")

		chunk_size = file_size_bytes // chunks
		eprint("size of each main chunk:", chunk_size)
		remotes_range_headers_map = {}
		for r in remotes:
			remotes_range_headers_map[r] = []
		current_range_offset = int(0)
		while current_range_offset < file_size_bytes - 1: # range is 0 indexed so 1000 bytes ends at offset 999
			# distribution
			for r in remotes: # split file into range headers for each remote to process single or multi threaded
				if remotes_diskfree[r] > (chunk_size * threads_per_remote):
					if current_range_offset+chunk_size > file_size_bytes - 1:
						remotes_range_headers_map[r].append(f"{int(current_range_offset)}-")
						current_range_offset = file_size_bytes - 1 #done
						break
					else:
						remotes_range_headers_map[r].append(f"{int(current_range_offset)}-{int(current_range_offset+chunk_size)}")
						current_range_offset += chunk_size + 1
				else:
					eprint("WARNING: the number of threads per remote uses more space than the size for each chunk, splitting into smaller ones")
					for i in (chunk_size * threads_per_remote // remotes_diskfree[r]):
						subchunk_size = remotes_diskfree[r] // threads_per_remote
						if current_range_offset+subchunk_size > file_size_bytes - 1:
							remotes_range_headers_map[r].append(f"{int(current_range_offset)}-")
							current_range_offset =  file_size_bytes - 1 #done
							break
						else:
							remotes_range_headers_map[r].append(f"{int(current_range_offset)}-{int(current_range_offset + subchunk_size)}")
							current_range_offset += subchunk_size + 1
		if verbose:
			eprint("chunks range headers map:")
			for r in remotes:
				eprint(r, remotes_range_headers_map[r])

		jobs_map = {}
		job_ids_todo = []
		job_ids_local_done = [] # jobids that are both done remotely and transferred locally
		jobs_per_remote = {}
		job_ids_completely_done = [] # jobids that's finished local transfer

		for r in remotes:
			jobs_per_remote[r] = {} #{jid:process}
		# split remotes range headers into a list of jobs
		for r in remotes_range_headers_map:
			for dl_range in remotes_range_headers_map[r]:
				job_id = f"{r}_{dl_range}"
				jobs_map[job_id] = (r, dl_range) # tuples of host, range
				job_ids_todo.append(job_id)

		all_job_ids = job_ids_todo.copy()
		scp_jobs_local_procs = {} #jobid:p

		# turn user supplied headers into curl arguments
		headers_arg = ""
		for h in headers:
			headers_arg += f"-H '{h}' "

		eprint(f"starting download in {chunks} main chunks, total {len(job_ids_todo)} jobs...")
		while len(job_ids_completely_done) != len(all_job_ids):

			try:
				job_id = random.choice(job_ids_todo)
				r, dl_range = jobs_map[job_id]
				eprint(f"jobs on {r}: {len(jobs_per_remote[r])}")
				if len(jobs_per_remote[r]) < threads_per_remote:
					eprint(f"starting new job on {r}: {job_id}")
					p = subprocess.Popen(["ssh", r, f"curl {headers_arg} -H 'Range: bytes={dl_range}' {url} > {get_filename_from_jobid(job_id)}"], stdout=PIPE, stderr=PIPE)
					jobs_per_remote[r][job_id] = p
					# dequeue the job
					job_ids_todo.remove(job_id)

				
			except IndexError: # out of remote jobs to do
				pass

			# check status
			for r in remotes:
				for jid in jobs_per_remote[r]:
					try:
						out,err = jobs_per_remote[r][jid].communicate(timeout=1)
						eprint(out.decode())
						eprint(err.decode())
					except subprocess.TimeoutExpired:
						pass
					ret = jobs_per_remote[r][jid].poll()
					if ret != None: #process done!
						if jid not in scp_jobs_local_procs:
							eprint(f"remote job {jid} done, retcode {ret}. Starting scp:")
							p = subprocess.Popen(["scp", f"{r}:{get_filename_from_jobid(jid)}", outdir+"/"],stdout=PIPE, stderr=PIPE)
							scp_jobs_local_procs[jid] = p

			

			# check local scp 
			scp_jobs_to_delete = []
			for jid in scp_jobs_local_procs:
				try:
					out,err = scp_jobs_local_procs[jid].communicate(timeout=1)
					eprint(out.decode())
					eprint(err.decode())
				except subprocess.TimeoutExpired:
					pass
				if scp_jobs_local_procs[jid].poll() != None:
					eprint(f"{jid} scp transfer done, deleting remote file..")
					r = jid.split("_")[0]
					subprocess.check_output(["ssh", r, f"rm {get_filename_from_jobid(jid)}"])
					del jobs_per_remote[r][jid]
					scp_jobs_to_delete.append(jid)
					job_ids_completely_done.append(jid)
					eprint(f"TOTAL PROGRESS: [{(len(job_ids_completely_done) / len(all_job_ids))*100:.2f}%]")
			for jid in scp_jobs_to_delete:
				del scp_jobs_local_procs[jid]


		# waiting on all files to scp across..

		if len(glob.glob(f"{outdir}/ff-*")) != len(all_job_ids):
			eprint("waiting on remaining files..")
			for jid in scp_jobs_local_procs:
				try:
					eprint(scp_jobs_local_procs[jid].communicate(timeout=1))
				except subprocess.TimeoutExpired:
					pass
				if scp_jobs_local_procs[jid].wait() != 0:
					print("some scp jobs finished with errors")
		filelist = glob.glob(f"{outdir}/ff-*")
		if len(glob.glob(f"{outdir}/ff-*")) != len(all_job_ids):
			print(f"Something went wrong and {len(all_job_ids) - len(filelist)} files are still missing..")

	filelist = glob.glob(f"{outdir}/ff-*")
	
	def get_sortkey(s):
		return int(s.split("_")[1].split("-")[0])
	# sort by range headers
	files_range_sorted = sorted(filelist,key=get_sortkey)
	# join files in a way that saves disk space
	print("all chunk files sorted:", " ".join(files_range_sorted))
	save_filename = url.split("/")[-1]
	with open(os.path.join(outdir, save_filename), "wb") as sf:
		for chunkfile in files_range_sorted:
			with open(chunkfile, "rb") as cf:
				sf.write(cf.read())
			os.remove(chunkfile)

	if not merge_only:
		if os.path.getsize(os.path.join(outdir,save_filename)) != file_size_bytes:
			eprint("WARNING: file size doesn't match, something went wrong.")
	eprint(f"DONE: output written to {outdir}/{save_filename}, elapsed {(time.time() - start)/60} minutes")



if __name__ == "__main__":
	main()
	print()

