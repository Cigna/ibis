import re
import sys
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


class ZookeeperLocks(object):
    """
    Zookeeper lock class that finds and removes locks
    from a Hive object
    Adapted from here:
      https://etl.svbtle.com/removing-database-level-locks-in-hive
    """
    def __init__(self, hosts, database, table):

        self.zk_client = KazooClient(hosts=hosts)

        # TODO: Move this out of the init function
        # Currently, this only looks at the table level and
        # assumes that the locks will be under there -
        # partitions tables will have nested dirs for the
        # partition (eg. domain=x, table=y)
        self.startup()

        self.database = database
        self.table = table

    def startup(self):
        # https://kazoo.readthedocs.io/en/latest/async_usage.html
        # returns immediately
        event = self.zk_client.start_async()
        # Wait for 30 seconds and see if we're connected
        event.wait(timeout=20)

        if not self.zk_client.connected:
            # Not connected, stop trying to connect
            self.zk_client.stop()
            raise Exception("Unable to connect.")

    def shutdown(self):
        """
        Close the connection to Zookeeper
        """
        self.zk_client.stop()

    def show_all_children(self):
        """
        Get a list of child nodes of a path.
        :return: List of child node names
        """
        return self.zk_client.get_children('/')

    def get_hive_namespace(self):
        """
        Find the Hive Zookeeper name space in all the
        children of the Zookeeper
        :return: string, name of Hive Name Space
        """
        hive_namespace = None
        for child in self.show_all_children():
            match = re.search(r'hive_zookeeper_namespace_hive.?', child)
            if match:
                hive_namespace = match.group()
                break
        return hive_namespace

    def path_setup(self):
        """setup path"""
        return "/{0}/{1}".format(self.get_hive_namespace(), self.database)

    def my_rec(self, name_of_lock):
        """
        Recursively goes through and remove locks
        at all levels. Locks can be at the table
        or at a partition level. In the case of
        some tables, we are 2 partitions deep.
        It is also possible to have more than 1
        lock on a given partition.
        """
        # we are getting unicode
        # all locks start with LOCK-
        locks_deleted = []
        if 'LOCK-' in name_of_lock.encode('ascii', 'ignore'):
            try:
                self.zk_client.delete(name_of_lock)
                print 'Deleted lock: {0}'.format(name_of_lock)
                return "Deleted " + name_of_lock
            except NoNodeError:
                print "No node error - delete lock: {0}".format(name_of_lock)
        else:
            new_locks = ""
            print "Need to go deeper", name_of_lock
            try:
                new_locks = self.zk_client.get_children(name_of_lock + "/")
                # this will return a list, but finish
                # if the list is empty
            except NoNodeError:
                print "No node error - get_children: {0}".format(name_of_lock)
            if len(new_locks) > 0:
                for a_lock in new_locks:
                    # for the next depth, go through
                    # and remove anything that starts
                    # with "LOCK-"
                    deleted_lock = self.my_rec(name_of_lock + "/" + a_lock)
                    locks_deleted.append(deleted_lock)
            else:
                return "All done"
        return locks_deleted

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "command line args are not proper"
        sys.exit(1)
    view_info = sys.argv[1]
    zookeeper_hosts = sys.argv[2]  # zookeeper quorom

    with open(view_info, 'rb') as view_files:
        for line in view_files:
            database, table = line.split(' ')
            zkl = ZookeeperLocks(zookeeper_hosts,
                                 database.strip(),
                                 table.strip())
            current_db = zkl.path_setup()
            print '-' * 100
            print 'Looking for locks on {0}.{1}'.format(database.strip(),
                                                        table.strip())
            zkl.my_rec(current_db)
            zkl.shutdown()
    print '-' * 100
