#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import yaml

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def _check_args():
    if len(sys.argv) < 2:
        print "usage: python _check_env.py <env_file>"
        exit(1)


def _load_env_list():
    try:
        env_file = open(os.path.join(SCRIPT_DIR, 'env-list.yaml'))
    except IOError:
        print 'No env-list.yaml found'
        exit(1)
    return yaml.load(env_file)


def _load_env():
    result = {}
    inputfile = os.path.join(SCRIPT_DIR, sys.argv[1])
    try:
        with open(inputfile) as f:
            for line in f.readlines():
                key, value = line.strip().split('=')
                result[key] = value
            return result
    except IOError:
        print 'No such file: ' + inputfile
        exit(1)


def _type_match(var, type_str):
    if type_str == 'string':
        return type(var) == str
    elif type_str == 'int':
        try:
            int(var)
            return True
        except Exception:
            return False
    elif type_str == 'float':
        try:
            float(var)
            return True
        except Exception:
            return False
    print 'unknown env type %s' % type_str
    return False


def _get_value_def(key, env_list):
    for p in env_list['envs']:
        if p['name'] == key:
            return p['type']


def _check(env=dict, env_list=dict):
    success = True
    key_list = [p['name'] for p in env_list['envs']]
    key_list_required = [p['name'] for p in env_list['envs'] if p['required']]
    # any one missing?
    for required_key in key_list_required:
        if required_key not in env.keys():
            print 'Missing required env %s' % required_key
            success = False

    for key in env:
        # any unrecognized?
        if key not in key_list:
            print 'Unrecognized env %s. Available: %s' % (key, ' '.join(key_list))
            success = False
        # any type error?
        if key in key_list and not _type_match(env[key], _get_value_def(key, env_list)):
            print 'env %s value type error: expected %s but got %s' % (key, _get_value_def(key, env_list), type(env[key]))
            success = False
    if success:
        print 'env check success'
    return success


if __name__ == '__main__':
    _check_args()
    env_list = _load_env_list()
    env = _load_env()
    # print env_list
    # print env
    success = _check(env, env_list)
    exit(0 if success else 1)
