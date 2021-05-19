#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Find dependants of a Python package"""
import pipdeptree


import logging
import pip
import pkg_resources
import sys

__program__ = 'dependants.py'


def get_dependants(target_name):
    for package in pip.get_installed_distributions():
        for requirement_package in package.requires():
            requirement_name = requirement_package.project_name
            if requirement_name == target_name:
                package_name = package.project_name
                yield package_name


# configure logging
logging.basicConfig(format='%(levelname)s: %(message)s',
                    level=logging.INFO)

target_name = 'deeppavlov'
#try:
#    target_name = sys.argv[1]
#except IndexError:
#    logging.error("missing package name")
#    sys.exit(1)

try:
    pkg_resources.get_distribution(target_name)
except pkg_resources.DistributionNotFound:
    logging.error("'%s' is not a valid package", target_name)
    sys.exit(1)

print(list(get_dependants(target_name)))

