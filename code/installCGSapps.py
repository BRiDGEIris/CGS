#!/usr/bin/python
__author__ = 'CGS'

import os, shutil, sys, distutils.core, subprocess

# Some configuration needed for this file
apps_directory = ""
apps = {"variants": "apps/variants", "patients": "apps/patients"}
PRODUCTION = False

# TODO: better management of errors
# Some basic checks
if os.getuid() != 0:
    sys.exit("This program requires super user privileges.")

if len(sys.argv) <= 1:
    sys.exit("Please, give the name of the app you want to install. Choose among the followings: " +
        str(apps.keys()))

if sys.argv[0] != "installCGSapps.py" and "/" in sys.argv[0]:
    # If the script was not launch in the current directory, we have to make some modifications
    tmp = sys.argv[0].split("/")
    script_name = tmp.pop()
    app_directory_prefix = sys.argv[0].replace("/"+script_name,"/")
else:
    app_directory_prefix = ""

# We take the folder where hue is installed
try:
    hue_directory = subprocess.Popen("whereis hue", stdin=False, shell=True, stdout=subprocess.PIPE)
    hue_directory = str(hue_directory.communicate()[0]).split(" ")[2].strip()
except:
    hue_directory = "/usr/lib/hue"

if not os.path.exists(hue_directory) and "HUE_DIRECTORY" in os.environ:
    hue_directory = os.environ["HUE_DIRECTORY"]

if os.path.exists(hue_directory) and not os.path.exists(hue_directory+"/myapps"):
    try:
        os.makedirs(hue_directory+"/myapps")
    except:
        sys.exit("Impossible to create the folder 'myapps' in '"+hue_directory+"'.")

apps_directory = hue_directory + "/myapps"
# Some basic checks first
if not os.path.exists(hue_directory):
    sys.exit("This installation file did not find the hue directory, please create a HUE_DIRECTORY environment"
             "variable.")

# We install each application
aborted = 0
for i in xrange(1, len(sys.argv)):
    app_name = sys.argv[i]
    if not app_name in apps:
        sys.exit("Invalid app name. Choose among the followings: "+str(apps.keys()))

    if not os.path.exists(app_directory_prefix+apps[app_name]):
        sys.exit("It seems the source of the app '"+app_name+"' is missing from the uncompressed zip.")

    # We try to delete the eventual old folder
    app_directory = apps_directory+"/"+app_name
    if os.path.exists(app_directory):
        if PRODUCTION == True:
            reinstall = raw_input("It seems the '"+app_name+"' already exists. Do you want to reinstall it [Y/n]?")
        else:
            reinstall = "Y"

        if reinstall != "Y" and reinstall != "y":
            print("Installation of '"+app_name+"' aborted.")
            aborted += 1
            continue
        else:
            try:
                shutil.rmtree(app_directory)
            except Exception as e:
                print(e.message)
                sys.exit("Impossible to delete the folder "+app_directory+". Check the access rights.")

    # We create the app
    # TODO: we do not catch correctly the errors of 'subprocess'
    try:
        print("Creating the app '"+app_name+"'...")
        app_install = subprocess.Popen("cd " + apps_directory + " && " + hue_directory +
                                       "/build/env/bin/hue create_desktop_app " + app_name,
                                       stdin=False, shell=True, stdout=subprocess.PIPE)
        app_install.communicate()

        app_install = subprocess.Popen("cd " + apps_directory + " && python " + hue_directory +
                                       "/tools/app_reg/app_reg.py --install " + app_name,
                                       stdin=False, shell=True, stdout=subprocess.PIPE)
        app_install.communicate()
    except Exception as e:
        print(e.message)
        sys.exit("Error while creating the app...")

    # We copy the content of the application to the new directory
    app_src = app_directory_prefix+apps[app_name]
    try:
        print("Copying source code to app folder...")
        # App code
        distutils.dir_util.copy_tree(app_src, app_directory)

        # App static files
        if not os.path.exists(hue_directory+"/build/static/"+app_name):
            try:
                os.makedirs(hue_directory+"/build/static/"+app_name)
            except:
                sys.exit("Impossible to create the static folder '"+app_name+"' in '"+hue_directory+"/build/static/"+app_name+"'.")
        distutils.dir_util.copy_tree(app_src+"/src/"+app_name+"/static", hue_directory+"/build/static/"+app_name)

    except:
        sys.exit("Impossible to copy data from '"+app_src+"' to '"+app_directory+"'.")

    # We create a directory for the cgs system
    try:
        app_install = subprocess.Popen("hadoop fs -mkdir /user/cgs && hadoop fs -chmod -R 777 /user/cgs",
                                       stdin=False, shell=True, stdout=subprocess.PIPE)
        app_install.communicate()
    except Exception as e:
        print(e.message)
        sys.exit("Error while creating the /user/cgs directory on HDFS.")

# We restart hue
try:
    app_install = subprocess.Popen("service hue stop && service hue start", stdin=False, shell=True, stdout=subprocess.PIPE)
    app_install.communicate()
except Exception as e:
    print(e.message)
    sys.exit("Error while restarting hue.")

# The happy end
if aborted == 0:
    print("Installation successful.")
elif aborted != len(sys.argv) - 1:
    print("Installation of the 'non-aborted' apps successful.")
