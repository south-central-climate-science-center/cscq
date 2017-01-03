from celery.task import task
from subprocess import call
from dockertask import docker_task
import os, requests, zipfile
import netCDF4 as nc
from os.path import basename

@task()
def ncrcat(parameter,domain,experiment,model,ensemble,base_output='/data/static_web/sccsc_tasks',host="data.southcentralclimate.org"):
    """ 
    This task spins up a docker container. SSH key must be within celery worker.
    Args: 
        parameter - CMIP5 ESGF Parameter (eg. "tas")
        domain - CMIP5 ESGF Domain (eg. "mon" or "day")
        experiment - CMIP5 ESGF Experiment (eg. ["historical"])
        model - CMIP5 ESGF List of Models (eg. ["GFDL-CM3"])
        ensemble - CMIP5 ESGF List of Esemble (eg. ["r1i1p1"])

    """    
    #Task ID and result directory 
    task_id = str(ncrcat.request.id)
    resultDir = os.path.join(base_output, task_id,'cmip5')
    os.makedirs(resultDir)
    result={}
    if isinstance(model,basestring):
        model = [model]
    if isinstance(experiment,basestring):
        experiment = [experiment]
    if isinstance(ensemble,basestring):
        ensemble = [ensemble]
    
    for mdl in model:
        #Make Model directory
        out_dir = "%s/%s/%s" % (resultDir,parameter,mdl.replace('(','-').replace(')',''))
        os.makedirs(out_dir)
        os.chmod(out_dir,mode=0777)
        #make netcdf header folder
        os.makedirs("{0}/{1}".format(out_dir,"netcdf_header"))
        #Concatenate CMIP5 file
        docker_opts = "-v /data:/data:z -v /data1:/data1:z -v /data2:/data2:z "
        try:
            #Run all experiment and ensembles
            for exp in experiment:
                for ens in ensemble:
                    try:
                        files,times,outfile= get_cmip5_metadata(parameter,domain,exp,mdl,ens,host)
                        if outfile:
                            file1="{0}/{1}".format(out_dir,outfile)
                            docker_cmd1 = "ncrcat %s %s" % (" ".join(files),file1)
                            docker_cmd1 = docker_cmd1.replace('(','-').replace(')','')
                            #file2="{0}/{1}.header.txt".format(out_dir,outfile)
                            #docker_cmd2 = "ncdump -h {0} > {1}".format(file1,file2)
                            #docker_cmd1 = "{0};{1}".format(docker_cmd1,docker_cmd2)
                            result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd1,id=task_id)
                            #netcdf header
                            file2="{0}/{1}/{2}.header.txt".format(out_dir,"netcdf_header",outfile)
                            docker_cmd1 = "ncdump -h {0} > {1}".format(file1,file2)
                            result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd1,id=task_id)
                    except Exception as e:
                        e_file = open("{0}/{1}_{2}_{3}_error.txt".format(out_dir,mdl,exp,ens),"w")
                        e_file.write("{0}{1}".format("ERROR: While CMIP file collection. Please see below for error description.\n\n",str(e)))
                        e_file.close()
        except Exception as inst:
            e_file = open(out_dir + "/error.txt","w")
            e_file.write("%s%s" % ("Error: during files collection. Please see below for error description.\n\n",str(inst)))
            e_file.close()
    return "http://{0}/sccsc_tasks/{1}/".format(host,task_id)

def get_cmip5_metadata(parameter,domain,experiment,model,ensemble,host):
    # Web API Url
    url = "http://{0}/api/catalog/data/catalog/cmip5_file/.json".format(host)
    url_params ="?page_size=0&query={'filter':{'variable':'%s','domain':'%s','experiment':'%s','ensemble':'%s','model':'%s'},'sort':[('time',1),('version',-1)]}"
    url = "%s%s" % (url,url_params % (parameter,domain,experiment,ensemble,model))
    response =requests.get(url)
    data = response.json()
    files=[]
    times=[]
    filename=None
    version=None
    for row in data['results']:
        #only return latest version
        if not version:
            version=row['version']
        if version==row['version']:
            filename=row['filename']
            times.extend(row['time'].split('-'))
            files.append(row['local_file'])
    files.sort()
    times.sort()
    try:
        filename="%s_%s-%s.%s" % ("_".join(filename.split('_')[:-1]),times[0],times[-1],filename.split('.')[-1])
    except:
        filename=None
    return files,times,filename

def merge_with_time(file1,file2):
    data1 = nc.Dataset(file1)
    data2 = nc.Dataset(file2)
    #rcp_starttime = data2.variables['time'][:][0]
    historical_endtime = data1.variables['time'][:][-1]
    try:
        idxvalue=0
        for itm in list(data2.variables['time'][:]):
            if float(itm) > float(historical_endtime):
                break
            idxvalue += 1
        if idxvalue==0:
            return None
        return "ncea -F -d time,%d, %s"  % (idxvalue,file2)
        #idxvalue = list(data2.variables['time'][:]).index(historical_endtime) + 1
        #return "ncea -F -d time,%d, %s"  % (idxvalue,file2) 
    except:
        return None

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
