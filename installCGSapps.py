#!/usr/bin/python
__author__ = 'CGS'

import os, shutil, sys, distutils.core, subprocess

# TODO: better management of errors
if not 'SUDO_UID' in os.environ.keys():
    sys.exit("This program requires super user privileges.")

# Some configuration needed for this file
apps_directory = ""
apps = {"cgs-app": "apps/cgs-app", "calculator": "apps/calculator"}

# We take the folder where hue is installed
try:
    hue_directory = subprocess.Popen("whereis hue", stdin=False, shell=True, stdout=subprocess.PIPE)
    hue_directory = str(hue_directory.communicate()[0]).split(" ")[2].strip()
except:
    hue_directory = "/usr/lib/hue"

if os.path.exists(hue_directory) and not os.path.exists(hue_directory+"/myapps"):
    try:
        os.makedirs(hue_directory+"/myapps")
    except:
        sys.exit("Impossible to create the folder 'myapps' in '"+hue_directory+"'.")

apps_directory = hue_directory + "/myapps"
# Some basic checks first
if not os.path.exists(hue_directory):
    sys.exit("This installation file did not find the hue apps directory, please edit the variable 'hue_directory'"
             " in this install.py file.")

if len(sys.argv) <= 1:
    sys.exit("Please, give the name of the app you want to install as 1th argument. Choose among the followings: " +
             str(apps.keys()))

app_name = sys.argv[1]
if not app_name in apps:
    sys.exit("Invalid app name. Choose among the followings: "+str(apps.keys()))

if not os.path.exists(apps[app_name]):
    sys.exit("It seems the source of the app '"+app_name+"' is missing from the uncompressed zip.")

# We try to delete the eventual old folder
app_directory = apps_directory+"/"+app_name
if os.path.exists(app_directory):
    reinstall = raw_input("It seems the "+app_name+" already exists. Do you want to reinstall it [Y/n]?")
    if reinstall != "Y" and reinstall != "y":
        sys.exit("Installation aborted.")
    else:
        try:
            shutil.rmtree(app_directory)
        except Exception as e:
            print(e.message)
            sys.exit("Impossible to delete the folder "+app_directory+". Check the access rights.")

# We create the app
# TODO: we do not catch correctly the errors of 'subprocess'
try:
    print("Creating the app...")
    app_install = subprocess.Popen("cd " + apps_directory + " && " + hue_directory +
                                   "/build/env/bin/hue create_desktop_app " + app_name,
                                   stdin=False, shell=True, stdout=subprocess.PIPE)
    app_install.communicate()

    app_install = subprocess.Popen("cd " + apps_directory + " && python " + hue_directory +
                                   "/tools/app_reg/app_reg.py --install " + app_name +
                                   " && service hue restart", stdin=False, shell=True, stdout=subprocess.PIPE)
    app_install.communicate()
except Exception as e:
    print(e.message)
    sys.exit("Error while creating the app...")

# We copy the content of the application to the new directory
app_src = apps[app_name]
try:
    print("Copying source code to app folder...")
    distutils.dir_util.copy_tree(app_src, app_directory)
except:
    sys.exit("Impossible to copy data from '"+app_src+"' to '"+app_directory+"'.")

# The happy end
print("Installation successful.")