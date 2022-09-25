import shutil
import os
import sys
from os.path import exists, join, basename
from collections import deque
from time import sleep
import threading
import redfish
import keyring
from fabric import Connection, Config
import patchwork.transfers
import subprocess
import socket
import yaml
from natsort import natsorted
from contextlib import redirect_stdout

USERNAME = keyring.get_password('server', 'username')
PASSWORD = keyring.get_password('server', 'password')

CONFIG = Config(overrides={'sudo': {'password': PASSWORD}})

DATA_FOLDER = '/home/jorisg/data'
PROJECTS_FOLDER = '/home/jorisg/projects'
JOB_FOLDER = '/home/jorisg/jobs'
RETURN_FOLDER = '/home/jorisg/return_data'
MAX_IDLE = 2 #how many idle cicles to allow (2min per cycle)

#{server_id: (ip, ilo_ip, RAM, CPU, text_color)
SERVERS = {1: ('10.0.1.1', '10.0.2.1', 128, 20, 'blue'),
           2: ('10.0.1.2', '10.0.2.2', 128, 20, 'green'),
           3: ('10.0.1.3', '10.0.2.3', 128, 20, 'yellow'),
           4: ('10.0.1.4', '10.0.2.4', 128, 20, 'red'),
           5: ('10.0.1.5', '10.0.2.5', 64, 20, 'cyan')}

SERVER_HOST = '192.168.1.200' # ip of the controller in the home network
SERVER_PORT = 2080

fmt = { #for formatting text outputs
   'purple': '\033[95m',
   'cyan': '\033[96m',
   'darkcyan': '\033[36m',
   'blue': '\033[94m',
   'green': '\033[92m',
   'yellow': '\033[93m',
   'red': '\033[91m',
   'bold': '\033[1m',
   'underline': '\033[4m',
   'end': '\033[0m'
}

class Server():
    def __init__(self, id, ip, ilo_ip, available_RAM, available_CPU, color):
        self.id = id
        self.ip = ip
        self.ilo_ip = ilo_ip
        self.available_RAM = available_RAM
        self.available_CPU = available_CPU
        self.color = fmt[color]
        self.idle_cycles = 0

        self.power = 'unknown'
        self.status_ok = True
        self.jobs = []
        self.job_lock = threading.Lock()

    #runs the job on the server
    #output: None
    #input: job object
    def run_job(self, job):
        print(self.color + f'job {job.id} started on server {self.id}' + fmt['end'])
        self.idle_cycles = 0
        if exists(join(RETURN_FOLDER, str(job.id))):
            shutil.rmtree(join(RETURN_FOLDER, str(job.id)))

        c = Connection(host=self.ip, user=USERNAME, config=CONFIG)
        patchwork.transfers.rsync(c, join(DATA_FOLDER, job.data_folder), DATA_FOLDER, exclude=['.git', '.idea', 'return_data'], rsync_opts="-q")
        patchwork.transfers.rsync(c, join(PROJECTS_FOLDER, job.project_folder), PROJECTS_FOLDER, exclude=['.git', '.idea', 'return_data'], rsync_opts="-q")
        job.status = 'running'

        c1 = f'cd {join(PROJECTS_FOLDER, basename(job.project_folder))}'
        c5 = f'mkdir -p return_data/{job.id}'

        c2 = f'python3 -m venv {basename(job.project_folder)}'
        c3 = f'source {basename(job.project_folder)}/bin/activate'
        c4 = f'pip install -r requirements.txt 2> return_data/{job.id}/stderr.txt'
        c6 = f'echo "  server_id:" > return_data/{job.id}/log.txt'
        c7 = f'echo "{self.id}" >> return_data/{job.id}/log.txt'

        c8 = f'echo "  start_time:" >> return_data/{job.id}/log.txt'
        c9 = f'echo `date "+%A %D %X"` >> return_data/{job.id}/log.txt'
        c10 = f'echo "  end_time:" >> return_data/{job.id}/log.txt'
        c11 = f'echo `date "+%A %D %X"` >> return_data/{job.id}/log.txt'

        python_command = f'python3 {job.entry_point} --job_id {job.id}'
        for argument in job.arguments.keys():
            python_command += f' --{argument} {job.arguments[argument]}'
        python_command += f' > return_data/{job.id}/stdout.txt 2> return_data/{job.id}/stderr.txt'

        screen_command = ';'.join([c1, c5, c2, c3, c4, c6, c7, c8, c9, c10, c11, python_command, 'exit']) #c6, c7, c8, c9, c10, c11
        c.run(f"screen -S {job.id} -dm bash -c '{screen_command}'")

        while True:
            sleep(120)
            running_screens = c.run('screen -ls', warn=True, hide=True)
            if f".{job.id}\t(" not in running_screens.stdout and 'Socket' in running_screens.stdout:
                sleep(30)
                running_screens = c.run('screen -ls', warn=True, hide=True)
                if f".{job.id}\t(" not in running_screens.stdout and 'Socket' in running_screens.stdout:
                    break

        subprocess.run(["scp", '-rq',  f'{USERNAME}@{self.ip}:{join(PROJECTS_FOLDER, basename(job.project_folder), "return_data", str(job.id))}',
                        join(RETURN_FOLDER, str(job.id))])

        with self.job_lock:
            self.jobs.remove(job.id)
            self.available_CPU += job.CPU_requirement
            self.available_RAM += job.RAM_requirement
        print(self.color + f'job {job.id} ended on server {self.id}' + fmt['end'])


    #starts the server up
    #input: None
    #output: None
    def start(self):
        self.power = 'starting up'
        REDFISH_OBJ = redfish.RedfishClient(base_url=f"https://{self.ilo_ip}", username=USERNAME,
                                            password=PASSWORD)
        REDFISH_OBJ.login(auth="session")
        if not REDFISH_OBJ.get('/redfish/v1/systems/1').obj.PowerState == 'On':
            REDFISH_OBJ.post('/redfish/v1/systems/1', {
                'Action': 'Reset',
                'ResetType': 'On',  #PushPowerButton for gentle off
            })
        REDFISH_OBJ.logout()

    #shuts the server down
    #input: None
    #output: None
    def shutdown(self):
        if self.power == 'off':
            return
        self.power = 'shutting down'
        c = Connection(host=self.ip, user=USERNAME, config=CONFIG)
        c.sudo('shutdown')
        c.close()

    #updates class variable for power status
    #input: None
    #output: None
    def update_power_status(self):
        REDFISH_OBJ = redfish.RedfishClient(base_url=f"https://{self.ilo_ip}", username=USERNAME,
                                            password=PASSWORD)
        REDFISH_OBJ.login(auth="session")
        power_status = REDFISH_OBJ.get('/redfish/v1/systems/1').obj.PowerState
        REDFISH_OBJ.logout()
        if power_status == 'Off':
            power_status = 'off'
        elif power_status == 'On':
            power_status = 'on'
            try:
                c = Connection(host=self.ip, user=USERNAME, config=CONFIG)
                c.run(':')
                c.close()
            except:
                power_status = 'transitioning'
        # print(f'self.power: {self.power} power_status: {power_status}')
        if self.power == 'shutting down' and power_status == 'off':
            self.power = 'off'
        elif (self.power == 'starting up' or self.power == 'off') and power_status == 'on':
            self.power = 'on'
        elif self.power == 'unknown' and power_status == 'on':
            self.power = 'on'
        elif self.power == 'unknown' and power_status == 'off':
            self.power = 'off'


class Job():
    def __init__(self, job_id, project_folder, data_folder, entry_point, arguments, CPU_requirement, RAM_requirement):
        self.id = job_id
        self.project_folder = project_folder
        self.data_folder = data_folder
        self.entry_point = entry_point
        self.arguments = arguments
        self.CPU_requirement = CPU_requirement
        self.RAM_requirement = RAM_requirement
        self.status = 'waiting'


#selects the server to run the job on and starts it if necessary
#input: job object
#output: None
def select_server(job):
    global servers

    running_servers_ids = [server_id for server_id, server in enumerate(servers) if server.power == 'on' and server.status_ok]
    server_to_use = -1
    if len(running_servers_ids) > 0:
        min_CPU = 1000
        for server_id in running_servers_ids:
            if servers[server_id].available_RAM >= job.RAM_requirement and \
                servers[server_id].available_CPU >= job.CPU_requirement and \
                    servers[server_id].available_CPU < min_CPU:
                min_CPU = servers[server_id].available_CPU
                server_to_use = server_id
    if server_to_use != -1:
        with servers[server_to_use].job_lock:
            servers[server_to_use].jobs.append(job.id)
            servers[server_to_use].available_CPU -= job.CPU_requirement
            servers[server_to_use].available_RAM -= job.RAM_requirement
        return server_to_use

    starting_servers = [server_id for server_id, server in enumerate(servers) if server.power == 'starting up' and server.status_ok]
    if not len(starting_servers) > 0:
        off_servers = [server_id for server_id, server in enumerate(servers) if server.power == 'off' and server.status_ok]
        for server_id in off_servers:
            if servers[server_id].available_RAM >= job.RAM_requirement and \
            servers[server_id].available_CPU >= job.CPU_requirement:
                servers[server_id].start()
                break


    return server_to_use

#runs the job on a server selected through select_server()
#input: job object
#output: Boolean, whether the job could be assigned to a server
def run_job(job):
    global servers
    server_id = select_server(job)
    if server_id == -1:
        return False
    else:
        job_thread = threading.Thread(group=None, target=servers[server_id].run_job, args=(job,))
        job_thread.start()
        return True


#goes trough the job queue, starting jobs when possible
#input: None
#output: None
def process_queue():
    global job_queue, servers
    while(True):
        job_files = natsorted(os.listdir(JOB_FOLDER))
        #put new jobs in queue:
        for job_file in job_files:
            job_path = os.path.join(JOB_FOLDER, job_file)
            with open(job_path, 'r') as f:
                job_description = yaml.safe_load(f)

            job = Job(job_description['job_id'], job_description['project_folder'], job_description['data_folder'], job_description['entry_point'],
                      job_description['arguments'], job_description['CPU_requirement'], job_description['RAM_requirement'])

            job_queue.appendleft(job)
            os.remove(job_path)

        #update power status:
        for server in servers:
            server.update_power_status()

        #work trough queue:
        while(True):
            if len(job_queue) > 0:
                job = job_queue.pop()
            else:
                break
            job_submitted = run_job(job)
            if not job_submitted:
                job_queue.append(job)
                break
        for server in servers:
            if server.power == 'on' and len(server.jobs) == 0:
                server.idle_cycles += 1
                if server.idle_cycles > MAX_IDLE:
                    server.idle_cycles = 0
                    server.shutdown()
        sleep(120)  # 2min

#hosts the status website
#input: None
#output: None
def show_status():
    global job_queue, servers
    while True:
        #wait for client connections
        client_connection, client_address = server_socket.accept()

        #initalise response
        response = 'HTTP/1.0 200 OK\n\n <!DOCTYPE html> <html> <body>'

        #create the body of the page
        for server in servers:
            response += f'<h1>Server {server.id}</h1> ' \
                        f'power: {server.power} <br>' \
                        f'status_ok: {server.status_ok} <br>' \
                        f'available_CPU: {server.available_CPU} <br>' \
                        f'available_RAM: {server.available_RAM} <br>' \
                        f'idle_cycles: {server.idle_cycles} <br>' \
                        f'running_jobs: {server.jobs} <br>'
        response += f'<h1>Jobs in queue:</h1>' \
                    f'{len(job_queue)}'
        response += '</body> </html>'

        #send the response
        client_connection.sendall(response.encode())

        #close connection
        client_connection.close()



if __name__ == '__main__':

    job_queue = deque()
    servers = [Server(server_id, *SERVERS[server_id]) for server_id in SERVERS.keys()]
    for server in servers:
        server.update_power_status()

    queue_thread = threading.Thread(target=process_queue)
    queue_thread.start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    server_socket.listen(1)

    status_thread = threading.Thread(target=show_status)
    status_thread.start()




