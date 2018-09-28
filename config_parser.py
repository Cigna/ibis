import sys
import os
from ConfigParser import SafeConfigParser


def validates_cli_args():
    """Validates the given command-line arguments.
           First Argument: Property file name
           Second Argument: Section to be parsed in the property file
    """
    global property_file, section

    # Checks the number of cli arguments given
    if len(sys.argv) != 3:
        print 'Error: Invalid arguments passed!\nUsage: python \
        config_parser.py <property filename> <section name>'
        sys.exit(1)

    property_file = sys.argv[1]
    section = sys.argv[2]

    # Checks the existence of property file
    if not os.path.exists(property_file):
        print 'Error: Property file, {} doesn\'t exists!'.format(os.path.basename(property_file))
        sys.exit(1)


def parses_config():
    """Parses the config file."""
    parser = SafeConfigParser()
    parser.read(property_file)

    if not parser.has_section(section):
        print 'Error: Given section, {} not found!'.format(section)
        sys.exit(1)

    return ''.join(map((lambda x: x[0]+'='+x[1]+' '), parser.items(section)))


def main():
    """Main program starts here."""
    validates_cli_args()
    print parses_config()  # Properties will be used in the shell script


if __name__ == '__main__':
    main()
