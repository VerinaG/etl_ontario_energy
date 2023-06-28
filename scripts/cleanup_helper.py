import os
import shutil


def clean_local_directory():

    dir = ['intertie_load', 'output']
    for f in os.listdir('./data'):
        if f in dir: shutil.rmtree(os.path.join('./data', f))
