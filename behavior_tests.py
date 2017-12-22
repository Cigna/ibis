
# -*- coding: utf-8 -*-
import re
import sys
from lettuce.bin import main


def test_end():
    """searches for ./features and runs the update.feature file"""
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(main())


if __name__ == '__main__':
    test_end()
