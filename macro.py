#! /usr/bin/env python
#
# Macro substitution using regular expressions
#
# @author: Sreejith K
# Created On 12 July 2010

import sys
import os
import re

class Macro(object):
    def __init__(self, mapper={}, content=''):
        self.mapper = mapper
        self.content = content

    def substitute_all(self):
        for regex, replace_string in self.mapper.iteritems():
            self.substitute(regex, replace_string)

    def substitute(self, regex, replace_string):
        self.content = re.sub(regex, replace_string, self.content)

    def get_content(self):
        return self.content

    def __str__(self):
        if self.content:
            return self.content
        else:
            return 'No content provided'


if __name__ == '__main__':
    mapper = {
            '\[TYPE\]': 'System',
            '\[OS\]': 'Linux',
            '\[VERSION\]': '2.6'
            }

    content = """
    My [TYPE] configuration:
        Operating System: [OS]
        Kernel Version: [VERSION]
        [OS] is POSIX compliant
    """
    print 'Content:\n', content
    macro = Macro(mapper, content)

    macro.substitute_all()

    print 'Result:\n', macro.get_content()
