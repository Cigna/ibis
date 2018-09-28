'''
    acts as a model class for freq_ingest table. all the values are set and
    get using the setter and getter.
'''


class Freq_Ingest(object):

    '''
    acts as a model class for freq_ingest table. all the values are set and
    get using the setter and getter.
    '''

    def __init__(self, team_name=None, frequency=None, table=None,
                 activate=None):
        '''
        has default variables
        team_name - view/team of the team
        frequency - frequency in which it is scheduled
        table - full table name - usually databasename_tablename
        activate - flag to create view hql
        '''
        if team_name is None:
            self.__view_nm = None
        else:
            self.__view_nm = team_name[0]
        if table is None:
            self.__full_tb_nm = None
        else:
            self.__full_tb_nm = table[0]
        if frequency is None:
            self.__freq = None
        else:
            self.__freq = frequency[0]
        if activate is None:
            self.__activator = None
        else:
            self.__activator = activate[0]

    @property
    def view_nm(self):
        return self.__view_nm

    @view_nm.setter
    def view_nm(self, view_name):
        self.__view_nm = view_name

    @property
    def full_tb_nm(self):
        return self.__full_tb_nm

    @full_tb_nm.setter
    def full_tb_nm(self, full_tb_nm):
        self.__full_tb_nm = full_tb_nm

    @property
    def frequency(self):
        return self.__freq

    @frequency.setter
    def frequency(self, freq):
        self.__freq = freq

    @property
    def activator(self):
        return self.__activator

    @activator.setter
    def activator(self, activator):
        self.__activator = activator
