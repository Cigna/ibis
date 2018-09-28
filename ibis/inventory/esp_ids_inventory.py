"""ESP IDS inventory."""
import os
from mako.template import Template
from ibis.inventory.inventory import Inventory


class ESPInventory(Inventory):
    """Class used for managing the records of source table connections.
    and properties in the esp_ids table.
    """

    def __init__(self, *arg):
        """Init."""
        super(self.__class__, self).__init__(*arg)
        self.table = self.cfg_mgr.esp_ids_table

    def get_tables_by_id(self, appl_id):
        """Return a list of tables."""
        tables = []
        query = "select * from {tbl} where appl_id='{id}'"
        query = query.format(tbl=self.table, id=appl_id)
        results = self.get_rows(query)
        if results:
            for table in results:
                (appl_id, job_name, time, string_date, ksh_name, esp_domain,
                 domain, database, frequency, group, env) = table
                tbl_dict = {'appl_id': appl_id, 'job_name': job_name,
                            'time': time, 'string_date': string_date,
                            'ksh_name': ksh_name,
                            'esp_domain': esp_domain,
                            'domain': domain, 'db': database,
                            'frequency': frequency,
                            'environment': env, 'esp_group': group}
                tables.append(tbl_dict)
        return tables

    def insert(self, esp_dict):
        """Insert into esp ids table"""
        appl_id = esp_dict['appl_id']
        insert = False
        if not self.get_tables_by_id(appl_id):
            values = ("'{appl_id}', '{job_name}', '{time}', '{string_date}',"
                      " '{ksh_name}', '{esp_domain}'")
            values = values.format(
                appl_id=esp_dict['appl_id'], job_name=esp_dict['job_name'],
                time=esp_dict['time'],
                string_date=esp_dict['string_date'],
                ksh_name=esp_dict['ksh_name'],
                esp_domain=esp_dict['esp_domain'])

            query = ("insert into table {tbl} partition (domain='{domain}',"
                     " db='{db}', frequency='{freq}', esp_group='{group}',"
                     " environment='{env}') values ({values})")
            query = query.format(tbl=self.table,
                                 freq=esp_dict['frequency'].lower(),
                                 domain=esp_dict['domain'].lower(),
                                 db=esp_dict['db'].lower(),
                                 env=esp_dict['environment'].upper(),
                                 group=esp_dict['esp_group'].lower(),
                                 values=values)
            self.run_query(query, self.table)
            insert = True
            msg = 'Inserted new record {id} into {esp_table} table'
            msg = msg.format(id=appl_id, esp_table=self.table)
            self.logger.info(msg)
        else:
            msg = 'ID {id} NOT INSERTED into {esp_table}. Already exists!'
            msg = msg.format(id=appl_id, esp_table=self.table)
            self.logger.warning(msg)
        return insert, msg

    def gen_esp_id(self, freq, domain, db_name, group=''):
        """Generate a new ESP id if not exists else return existing.
        Args:
            freq: refresh frequency. Ex: weekly
            domain: domain name from request file
            db_name: database name from request file
            group: esp_group from request file
        """
        if group is None:
            group = ''
        # Normalize values
        freq = freq.lower()
        domain = domain.lower()
        db_name = db_name.lower()
        env_str = self.cfg_mgr.env
        group = group.lower()
        if freq not in self.cfg_mgr.frequencies_map.keys():
            warn_msg = ("Frequency: '{0}' is not valid "
                        "value to generate appl-id")
            warn_msg = warn_msg.format(freq)
            self.logger.warning(warn_msg)
            return None
        if group == '':
            query = ("select appl_id from {tbl} where frequency='{freq}'"
                     " and domain='{domain}' and db='{db}' and "
                     "environment='{env}' and esp_group is NULL")
            query = query.format(tbl=self.table, freq=freq.lower(),
                                 domain=domain.lower(),
                                 db=db_name.lower(), env=env_str.upper())
        else:
            # when esp_group is same, different tables/domain/db
            #  shall have the same appl_id
            query = ("select appl_id from {tbl} where environment='{env}'"
                     " and esp_group='{grp}'")
            query = query.format(tbl=self.table, env=env_str.upper(),
                                 grp=group.lower())

        result = self.get_rows(query)
        if not result:
            esp_id = self.calculate_esp_id(freq)
            esp_domain = self.cfg_mgr.big_data.lower()
            job_name = "C1_" + self.cfg_mgr.big_data + "_" + domain + \
                       "_" + db_name + "_" + freq
            # Dummy values by default
            dates_map = {'daily': 'Every Day',
                         'weekly': 'Every Tuesday',
                         'biweekly': 'Twice a week',
                         'fortnightly': 'Every second Tuesday',
                         'monthly': '21st Every Month',
                         'quarterly': '28th Day of 3rd, 6th, 9th, '
                                      'and 12th Month',
                         'yearly': 'Once in 6 months'}
            ksh_name = '{domain}_{db}_{freq}'.format(domain=domain, db=db_name,
                                                     freq=freq)

            if group:
                ksh_name = '{esp_group}_{freq}'.format(esp_group=group,
                                                       freq=freq)

            esp_dict = {'appl_id': esp_id, 'job_name': job_name,
                        'time': '0:00', 'string_date': dates_map[freq],
                        'ksh_name': ksh_name, 'esp_domain': esp_domain,
                        'domain': domain, 'db': db_name, 'frequency': freq,
                        'environment': env_str, 'esp_group': group}
            success, msg = self.insert(esp_dict)
            self.logger.warning(msg)
            return esp_id
        return result[0][0]

    def calculate_esp_id(self, freq):
        """Calculate a new ESP id.
        Example: FAKEW004
        FAKE: Convention for Big Data
        W: Frequency, in this case Weekly (can be: D, W, M, Q)
        00: increments everytime - 00, 01, ... 0A...0Z, 10
        4: Environment, in this case INT (1 = Dev, 4 = Int, 6= Prod)
        """
        # Normalize values
        freq = freq.lower()
        # Convention for 'Big Data'
        esp_prefix = self.cfg_mgr.big_data
        # Frequency mapping.
        freq_prefix = self.cfg_mgr.frequencies_map[freq]
        # Environment mapping.
        env_num = self.cfg_mgr.environment_map
        query = ("select DISTINCT(SUBSTR(appl_id,length(appl_id)-3,3)) from "
                 "{table};")
        query = query.format(table=self.table)
        result = self.get_rows(query)
        result = [i[0] for i in result]
        current_seqs = []
        for _seq in result:
            if _seq[0].lower() == freq_prefix.lower():
                current_seqs.append(_seq[1:])
        next_seq = ESPSequence(current_seqs).next_sequence
        esp_id = esp_prefix + freq_prefix + next_seq + env_num
        return esp_id

    def gen_wld_tables(self, appl_id, tables, workflow_names):
        """Creates wld files for job scheduling using ESP for tables"""
        remaining_wld_jobs = []
        first_wld_job = None
        for index, table in enumerate(tables):
            job_name = table.domain + '_' + table.db_table_name
            ksh_name = workflow_names[index] + '.ksh'
            if index == len(tables) - 1:
                # last job
                next_job_name = 'APPLEND1.' + appl_id
            else:
                next_table = tables[index + 1]
                next_job_name = next_table.domain + '_' + \
                    next_table.db_table_name
            job = WldJob(job_name, ksh_name, next_job_name)
            if index == 0:
                # first job
                first_wld_job = job
            else:
                remaining_wld_jobs.append(job)
        wld_file_name = appl_id
        return self.write_wld_file(appl_id, wld_file_name, first_wld_job,
                                   remaining_wld_jobs)

    def gen_wld_subworkflow(self, appl_id, appl_refs, num_jobs):
        """Creates wld files for job scheduling using ESP for sub workflows"""
        job_name_prefix = appl_refs['job_name']
        ksh_name_prefix = appl_refs['ksh_name']
        remaining_wld_jobs = []
        first_wld_job = None
        for index in reversed(range(num_jobs)):
            job_name = job_name_prefix + '_' + str(index + 1)
            ksh_name = ksh_name_prefix + '_' + str(index + 1) + '.ksh'
            if index == num_jobs - 1:
                # last job
                next_job_name = 'APPLEND1.' + appl_id
            else:
                next_job_name = job_name_prefix + '_' + str(index + 2)
            job = WldJob(job_name, ksh_name, next_job_name)
            if index == 0:
                # first job
                first_wld_job = job
            else:
                remaining_wld_jobs.append(job)
        wld_file_name = appl_id + '_subworkflow'
        return self.write_wld_file(appl_id, wld_file_name, first_wld_job,
                                   remaining_wld_jobs)

    def write_wld_file(self, appl_id, wld_file_name, first_wld_job,
                       remaining_wld_jobs):
        """Write WLD file"""
        template = Template(filename=self.cfg_mgr.wld_template_mako,
                            format_exceptions=True)
        wld_content = template.render(
            appl_id=appl_id,
            host_name_prefix=self.cfg_mgr.edge_node.split('.', 1)[0],
            first_wld_job=first_wld_job, remaining_wld_jobs=remaining_wld_jobs)
        file_name = '{name}.wld'.format(name=wld_file_name)
        output_file = os.path.join(self.cfg_mgr.files, file_name)
        with open(output_file, "wb") as wld_fh:
            wld_fh.write(wld_content)
        return file_name


class WldJob(object):
    """WLD file LINUX_JOB"""

    def __init__(self, job_name, script_name, next_job_name):
        """init"""
        self.job_name = job_name
        self.script_name = script_name
        self.next_job_name = next_job_name


class ESPSequence(object):
    """Generates sequence of alphanumeric strings of length 2"""

    custom_alphanumerics = '0123456789abcdefghijklmnopqrstuvwxyz'

    def __init__(self, current_seqs=None):
        """Init
        Args:
            current_seqs: list of two digit sequences. 00, 01, 0A, ZZ
        """
        current_seqs = [seq.lower() for seq in current_seqs]
        self.current_seqs = current_seqs
        self.alpha_nums = []
        for each in self.custom_alphanumerics:
            self.alpha_nums.append(each.lower())
        self.total = len(self.alpha_nums)

    def get_next_char(self, char_s):
        """returns the next char assuming its a circular list"""
        next_char = ''
        if char_s.lower() == 'z':
            next_char = '0'
        else:
            i = self.custom_alphanumerics.index(char_s)
            next_char = self.alpha_nums[i + 1]
        return next_char

    @property
    def next_sequence(self):
        """Returns a sequence that is unique
           based on existing sequences.
        """
        next_first = next_second = ''
        if not self.current_seqs:
            return '00'

        self.current_seqs.sort()
        last_item = self.current_seqs[-1]
        first, second = last_item[0], last_item[1]
        if second == 'z':
            next_second = self.get_next_char(second)
            next_first = self.get_next_char(first)
        else:
            next_second = self.get_next_char(second)
            next_first = first
        return next_first.upper() + next_second.upper()
