from celery.task import task
from subprocess import call
from dockertask import docker_task
import os, requests
import netCDF4 as nc
@task()
def ncrcat(parameter,domain,experiment,model,ensemble,base_output='/data/static_web/sccsc_tasks'):
    """ 
    This task spins up a docker container. SSH key must be within celery worker.
    Args: 
        parameter - CMIP5 ESGF Parameter (eg. "tas")
        domain - CMIP5 ESGF Domain (eg. "Amon" or "day")
        experiment - CMIP5 ESGF Experiment (eg. "historical")
        model - CMIP5 ESGF List of Models (eg. ["GFDL-CM3"])
        ensemble - CMIP5 ESGF Esemble (eg. "r1i1p1")

    """    
    #Task ID and result directory 
    task_id = str(ncrcat.request.id)
    resultDir = os.path.join(base_output, task_id,'cmip5')
    os.makedirs(resultDir)
    result={}
    if isinstance(model,basestring):
        model = [model]
    
    for mdl in model:
        #Make Model directory
        out_dir = "%s/%s/%s" % (resultDir,parameter,mdl)
        os.makedirs(out_dir)
        #Concatenate CMIP5 file
        docker_opts = "-v /data:/data"
        try:
            #Get CMIP5 Metadata
            if experiment =="historical":
                files,times,outfile= get_cmip5_metadata(parameter,domain,experiment,mdl,ensemble)
                docker_cmd = "ncrcat %s %s" % (" ".join(files), "%s/%s" % (out_dir,outfile))
                result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
            else:   
                #Historical
                files,times,outfile= get_cmip5_metadata(parameter,domain,"historical",mdl,ensemble)
                if outfile:
                    file1="%s/%s" % (out_dir,outfile)
                    docker_cmd1 = "ncrcat %s %s" % (" ".join(files),file1)
                    result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd1,id=task_id)
                #RCP future projection
                files1,times1,outfile1= get_cmip5_metadata(parameter,domain,experiment,mdl,ensemble)
                if outfile1:
                    file2="%s/%s" % (out_dir,outfile1)
                    docker_cmd2 = "ncrcat %s %s" % (" ".join(files1),file2)
                    result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd2,id=task_id)
                
                #Splice file to the correct merge point.
                if outfile and outfile1:
                    file3 = "%s/%s_%s-%s.nc" % (out_dir,"_".join(outfile1.split('_')[:-1]),"spliced",times1[-1])
                    merge = merge_with_time(file1,file2)
                    file4 = "%s/%s_%s-%s.nc" % (out_dir,"_".join(outfile1.split('_')[:-1]),times[0],times1[-1])
                    if merge:
                        docker_cmd3 = "%s %s" % (merge ,file3)
                        result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd3,id=task_id)
                        docker_cmd4 = "ncrcat %s %s %s" % (file1,file3,file4)
                        result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd4,id=task_id)
                    else:
                        docker_cmd4 = "ncrcat %s %s %s" % (file1,file2,file4)
                        result = docker_task(docker_name="sccsc/netcdf",docker_opts=docker_opts,docker_command=docker_cmd4,id=task_id)
        except Exception as inst:
            e_file = open(out_dir + "/error.txt","w")
            e_file.write("%s%s" % ("Error: during files collection. Please see below for error description.\n\n",str(inst)))
            e_file.close()
            raise
    return "http://%s/sccsc_tasks/%s" % (result['host'],result['task_id'])

def get_cmip5_metadata(parameter,domain,experiment,model,ensemble):
    # Web API Url
    url = "http://data.southcentralclimate.org/api/catalog/data/data_portal/cmip5_file/.json"
    url_params ="?page_size=0&query={'spec':{'variable':'%s','domain':'%s','experiment':'%s','ensemble':'%s','model':'%s'},'$orderby':{'time':1}}"
    url = "%s%s" % (url,url_params % (parameter,domain,experiment,ensemble,model))
    response =requests.get(url)
    data = response.json()
    files=[]
    times=[]
    filename=None
    for row in data['results']:
        filename=row['filename']
        times.extend(row['time'].split('-'))
        files.append(row['local_file'])
    files.sort()
    times.sort()
    try:
        filename="%s_%s-%s.nc" % ("_".join(filename.split('_')[:-1]),times[0],times[-1])
    except:
        filename=None
    return files,times,filename

def merge_with_time(file1,file2):
    data1 = nc.Dataset(file1)
    data2 = nc.Dataset(file2)
    historical_endtime = data1.variables['time'][:][-1]
    try:
        idxvalue = list(data2.variables['time'][:]).index(historical_endtime) + 1
        return "ncea -F -d time,%d, %s"  % (idxvalue,file2) 
    except:
        return None
