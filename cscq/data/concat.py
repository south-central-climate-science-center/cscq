from celery.task import task
from subprocess import call
import os, requests

@task()
def ncrcat(parameter,domain,experiment,model,ensemble,base_output='/data/static_web/sccsc_tasks'):
    """ 
    ncrcat performs a system call to ncrcat. NCO netcdf toolsmust be install on the host operating system.
    Args: 
        parameter - CMIP5 ESGF Parameter (eg. "tas")
        domain - CMIP5 ESGF Domain (eg. "Amon" or "day")
        experiment - CMIP5 ESGF Experiment (eg. "historical")
        model - CMIP5 ESGF Model (eg. "GFDL-CM3")
        ensemble - CMIP5 ESGF Esemble (eg. "r1i1p1")

    """    
    #Task ID and result directory 
    task_id = str(ncrcat.request.id)
    resultDir = os.path.join(base_output, task_id)
    os.makedirs(resultDir)
    # Web API Url
    url = "http://data.southcentralclimate.org/api/catalog/data/data_portal/cmip5_files/.json"
    url_params ="?page_size=0&query={'spec':{'variable':'%s','domain':'%s','experiment':'%s','ensemble':'%s','model':'%s'},'$orderby':{'time':1}}"
    url = "%s%s" % (url,url_params % (parameter,domain,experiment,ensemble,model))
    
    response =requests.get(url)
    data = response.json()
    files=[]
    times=[]
    filename=""
    for row in data['results']:
        filename=row['filename']
        times.extend(row['time'].split('-'))
        files.append("%s/%s.%s" % (row['path'],row['filename'],'nc'))
    times.sort()
    filename="%s_%s-%s.nc" % ("".join(filename.split('_')[:-1]),times[0],times[-1])
    files.sort()
    #Concatenate file
    call(['ncrcat','-O', "%s/%s" % (resultDir,filename)].extend(files))
    return "http://data.southcentralclimate.org/sccsc_tasks/%s/%s" % (task_id,"%s/%s" % (resultDir,filename))



    


