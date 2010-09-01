#! /usr/bin/env python
#
# Generate the Filesystem tree from a list of files
#
# @author: Sreejith K
# Created on 1st Sept 2010

import sys
import os


def add_to_tree(tree, filename):
    directory, filename = os.path.split(filename)
    if directory == '/' and filename == '':
        return
    if not tree.has_key(directory):
        tree[directory] = [filename]
    elif not filename in tree[directory]:
        tree[directory].append(filename)
    add_to_tree(tree, directory)


def generate_tree(filenames):
    tree = {}
    for filename in filenames:
        tree[filename] = []
        add_to_tree(tree, filename)
    return tree


if __name__ == '__main__':
    dir1 = '/home/sree/Downloads'
    dir2 = '/home/sree/src'
    files1 = map(lambda x: os.path.join(dir1, x), os.listdir(dir1)[:5])
    files2 = map(lambda x: os.path.join(dir2, x), os.listdir(dir2)[:5])
    print files1+files2
    tree = generate_tree(files1+files2)
    for dir, content in tree.iteritems():
        print dir, '\t', content
