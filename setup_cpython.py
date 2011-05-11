from distutils.core import setup, Extension
import shutil
import sys

"""
try:
	shutil.rmtree("./build")
except(OSError):
	pass
"""    
    
libs = []

if sys.platform != "win32":
    libs.append("stdc++")
    
if sys.platform == "win32":
    libs.append("ws2_32")


module1 = Extension('amysql',
                sources = ['amysql.c', 'io_cpython.c', 'capi.cpp', 'Connection.cpp', 'PacketReader.cpp', 'PacketWriter.cpp', 'SHA1.cpp'],
                include_dirs = ['./'],
                library_dirs = [],
                libraries=libs,
                define_macros=[('WIN32_LEAN_AND_MEAN', None)])
					
setup (name = 'amysql',
       version = '1.0',
       description = '',
       ext_modules = [module1])
       
       
