"""Parse input file"""
from ibis.utilities.utilities import Utilities


def parse_file_by_sections(section_file, header, required_keys,
                           optional_fields):
    """Split a file by a common header.
    checks if it contains complete sections using a list of required keys.
    Returns a list of each section as a dictionary and a message
    """
    # List used to store all sections
    all_sections = []
    msg = ''
    log_msg = ''
    try:
        lines = section_file.readlines()
        lines = Utilities.clean_lines(lines)
        last = len(lines) - 1
        # List used to store a section
        section = {}
        flag = False

        def add_to_dictionary():
            """compare keys"""
            if set(required_keys) <= set(section.keys()):
                # Add complete section to list
                all_sections.append(section)
            else:
                diff_keys = set(required_keys).difference(section.keys())
                missed_keys = ", ".join(sorted(diff_keys))
                err_msg = '\n\n' + '%' * 100
                err_msg += "\n\tMissing mandatory fields in file: {file}"
                err_msg += " \n\tMandatory fields: {mandatory}"
                err_msg += "\n\tMissing fields: {missed_keys}"
                err_msg += "\n\tEntered fields: {given_fields}"
                err_msg += '\n' + '%' * 100
                err_msg = err_msg.format(
                    file=section_file.name,
                    mandatory=', '.join(sorted(required_keys)),
                    missed_keys=missed_keys,
                    given_fields=', '.join(sorted(section.keys())))
                raise IndexError(err_msg)

            for each in section.keys():
                all_keys = required_keys + optional_fields
                if each not in all_keys:
                    extra_fields = sorted(
                        set(section.keys()).difference(all_keys))
                    err_msg = '\n\n' + '%' * 100
                    err_msg += '\n\tAllowed fields: {0}. '
                    err_msg += '\n\tEntered fields: {1}'
                    err_msg += '\n\tRemove these fields: {2}'
                    err_msg += '\n' + '%' * 100
                    err_msg = err_msg.format(', '.join(sorted(all_keys)),
                                             ', '.join(sorted(section.keys())),
                                             ', '.join(extra_fields))
                    raise IndexError(err_msg)

        header_found = False

        for i, line in enumerate(lines):
            # Check for non empty line
            if line:
                if line == header:
                    header_found = True
                    if flag:
                        add_to_dictionary()
                        section = {}
                else:
                    flag = True
                    # Split the line by the first : occurrence to get key/value
                    # and add it to the section dictionary
                    try:
                        r = line.split(':', 1)
                        if header_found:
                            col = r[0].strip()
                            val = r[1].strip()

                            # remove the condition after 2 weeks
                            if col.rstrip().lower() == 'domain':
                                log_msg = "Domain field is deprecated!!! "
                                log_msg += "Default domain database_i "
                                log_msg += "is assumed. Please stop providing"
                                log_msg += " Domain. Process will begin to"
                                log_msg += " fail in next 2 weeks."
                            # Add  key value pair to section dictionary
                            elif col.rstrip() not in section.keys():
                                val = val.rstrip()
                                val = val.replace('"', '\\"')
                                val = val.replace("'", "\\'")
                                section[col.rstrip()] = val
                            else:
                                err_msg = ("Duplicate Key value pair: '{0}':"
                                           " '{1}'\n{2} header missing?")
                                err_msg = err_msg.format(col, val, header)
                                err_msg = Utilities.print_box_msg(err_msg, 'x')
                                raise ValueError(err_msg)
                        else:
                            err_msg = 'Header {0} not found'.format(header)
                            err_msg = Utilities.print_box_msg(err_msg, 'x')
                            raise ValueError(err_msg)

                        if i == last:
                            add_to_dictionary()

                    except IndexError as err:
                        all_sections = None
                        msg = ("\n Verify the key-value pair in the"
                               " request file: {file}")
                        msg = msg.format(file=section_file.name)
                        msg = err.message + msg
                        raise ValueError(msg)
            else:
                # Empty line
                if i == last:
                    add_to_dictionary()
                else:
                    continue
        section_file.close()
    except IOError:
        msg = 'Error can\'t find file or read data from {file}'
        msg = msg.format(file=section_file.name)
        raise ValueError(msg)
    except IndexError as err:
        all_sections = None
        msg = err.message
        raise ValueError(msg)
    return all_sections, msg, log_msg
