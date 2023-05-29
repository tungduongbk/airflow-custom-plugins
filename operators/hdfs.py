import os
from typing import Dict, Optional, Callable
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from hooks.hdfs import HDFSHook


class HDFSException(AirflowException):
    pass


class HDFSOperator(BaseOperator):
    def __init__(self, command, op_kwargs, hook=HDFSHook, **kwargs):
        super(HDFSOperator, self).__init__(**kwargs)
        self.command = command
        self.op_kwargs = op_kwargs
        self.hook = hook

    def execute(self, context):
        hook = self.hook()
        client = hook.get_conn()
        if self.command == 'put':
            source = self.op_kwargs['source']
            dest_dir = self.op_kwargs['dest_dir']
            if not os.path.exists(source) or not dest_dir:
                print("Source: {} isn't existed, error!!!")
                exit(1)
            else:
                client.makedirs(dest_dir, mode=0o755)
                if os.path.isdir(source):
                    files = os.listdir(source)
                    for file in files:
                        source_file = os.path.join(source, file)
                        client.put(filename=source_file,
                                    path='{}/{}'.format(dest_dir, file))
                else:
                    file = source.split('/')[-1]
                    client.put(filename=source,
                               path='{}/{}'.format(dest_dir, file))
        elif self.command == 'rm':
            dest = self.op_kwargs['dest']
            recursive = self.op_kwargs.get('recursive', True)
            client.rm(path=dest, recursive=recursive)
        elif self.command == 'mv':
            source = self.op_kwargs['source']
            dest = self.op_kwargs['dest']
            client.mv(source, dest)
        else:
            raise HDFSException(
                "Unsupported HDFS command {}".format(self.command))


class PutHDFSOperator(BaseOperator):
    """
    This operator using to put local file or folder of data into a HDFS folder

    :param local_source: local absolute path of file or folder 
    :type local_source: str
    :param dest_dir: HDFS destination directory
    :type dest_dir: str
    :param file_conf: the configuration about storage file on HDFS include number of replication and blocksize (unit: byte)
    :type file_conf: dict (default: {'replication': 2, 'blocksize': 134217728})
    :param file_filter: a callcable that receive a file_name and return False if want to filter out this file
    :type file_conf: Callable (default: None)
    :param hook: Hook class that this operator based on
    :type hook: cls
    :param hdfs_conn_id: the connection ID of HDFS
    :type hdfs_conn_id: str
    :param hdfs_user: the user do this operator 
    :type hdfs_user : str (default: hadoop)
    """

    template_fields = ('local_source', 'dest_dir', 'file_conf', 'file_filter')

    def __init__(self,
        local_source: str,
        dest_dir: str,
        file_conf: Optional[Dict] = None,
        file_filter: Optional[Callable] = None,
        hook = HDFSHook,
        hdfs_conn_id: str = "hdfs_default",
        hdfs_user: str = "hadoop",
        **kwargs):
        super(PutHDFSOperator, self).__init__(**kwargs)
        self.hook = hook
        self.local_source = local_source
        self.dest_dir = dest_dir
        self.file_conf = file_conf
        self.file_filter = file_filter
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_user = hdfs_user

    def execute(self, context):
        self.log.info("Local files: {}".format(self.local_source))
        if not self.local_source:
            raise HDFSException('Source must be provided !!!')
        if not os.path.exists(self.local_source):
            raise HDFSException(
                f"Source {self.local_source} isn't existed !!!")
        if not self.dest_dir:
            raise HDFSException('Dest dir must be provided !!!')
        hook = self.hook(hdfs_conn_id=self.hdfs_conn_id, hdfs_user=self.hdfs_user)
        self.client = hook.get_conn()
        self.file_conf = self.file_conf if self.file_conf is not None else hook.get_file_conf()
        PutHDFSOperator._copyObjToDir(self.local_source, self.dest_dir, self.client, self.file_conf, self.file_filter)
    
    @staticmethod
    def _copyObjToDir(local_obj, hdfs_dir, client, file_conf, file_filter):
        if os.path.isdir(local_obj):
            PutHDFSOperator._copyDirToDir(local_obj, hdfs_dir, client, file_conf, file_filter)
        else:
            PutHDFSOperator._copyFileIntoDir(local_obj, hdfs_dir, client, file_conf, file_filter)

    @staticmethod
    def _copyDirToDir(local_dir, hdfs_dir, client, file_conf, file_filter):
        for o in os.listdir(local_dir):
            sub_local_obj = os.path.join(local_dir, o)
            if os.path.isdir(sub_local_obj):
                sub_hdfs_dir = os.path.join(hdfs_dir, o)
                PutHDFSOperator._copyDirToDir(sub_local_obj, sub_hdfs_dir, client, file_conf, file_filter)
            else:
                PutHDFSOperator._copyFileIntoDir(sub_local_obj, hdfs_dir, client, file_conf, file_filter)

    @staticmethod
    def _copyFileIntoDir(local_file, hdfs_dir, client, file_conf, file_filter):
        file_name = local_file.split('/')[-1]
        replication = file_conf.get('replication', 2)
        block_size = file_conf.get('blocksize', 134217728)
        hdfs_path = os.path.join(hdfs_dir, file_name)
        if file_filter is None or file_filter(file_name):
            client.makedirs(hdfs_dir, mode=0o755)
            client.put(filename=local_file,
                            path=hdfs_path,
                            replication=replication,
                            block_size=block_size)
            print(f'Put {local_file} to {hdfs_path}')


class RmHDFSOperator(BaseOperator):
    """
    This operator using to remove a HDFS file or folder
    :param path: HDFS file or folder path you want to remove
    :type path: str
    """

    template_fields = ('path',)

    def __init__(self, path: str, hook=HDFSHook, hdfs_conn_id: str = 'hdfs_default', **kwargs):
        super(RmHDFSOperator, self).__init__(**kwargs)
        self.hook = hook
        self.path = path
        self.hdfs_conn_id = hdfs_conn_id

    @staticmethod
    def _remove(client, path):
        client.rm(path, recursive=True)

    def execute(self, context):
        if not self.path:
            raise HDFSException('Path to remove must be provided !!!')
        hook = self.hook(hdfs_conn_id=self.hdfs_conn_id)
        client = hook.get_conn()
        try:
            RmHDFSOperator._remove(client, self.path)
            self.log.info(" ***** Removed hdfs files {}".format(self.path))
        except:
            self.log.warning("File or folder not found: {}".format(self.path))

class GetHDFSOperator(BaseOperator):
    """
    This operator using to get a HDFS file or folder to local 
    :param hdfs_path: HDFS path for copying from
    :type hdfs_path: str
    :param local_path: local path for moving to
    :type local_path: str
    """

    template_fields = ('hdfs_source', 'dest_dir')

    def __init__(self,
        hdfs_source: str,
        dest_dir: str,
        file_filter: Optional[Callable] = None,
        hook=HDFSHook,
        hdfs_conn_id: str = 'hdfs_default',
        **kwargs):

        super(GetHDFSOperator, self).__init__(**kwargs)
        self.hook = hook
        self.hdfs_source = hdfs_source
        self.dest_dir = dest_dir
        self.file_filter = file_filter
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):
        hook = self.hook(hdfs_conn_id=self.hdfs_conn_id)
        self.client = hook.get_conn()

        self.log.info("HDFS source: {}".format(self.hdfs_source))
        if not self.hdfs_source:
            raise HDFSException('Source must be provided !!!')
        if not self.client.exists(self.hdfs_source):
            raise HDFSException(
                f"Source {self.hdfs_source} isn't existed !!!")
        if not self.dest_dir:
            raise HDFSException('Dest dir must be provided !!!')
        
        GetHDFSOperator._copyObjToDir(self.hdfs_source, self.dest_dir, self.client, self.file_filter)
    
    @staticmethod
    def _copyObjToDir(hdfs_obj, local_dir, client, file_filter):
        if client.isdir(hdfs_obj):
            GetHDFSOperator._copyDirToDir(hdfs_obj, local_dir, client, file_filter)
        else:
            GetHDFSOperator._copyFileIntoDir(hdfs_obj, local_dir, client, file_filter)

    @staticmethod
    def _copyDirToDir(hdfs_dir, local_dir, client, file_filter):
        for sub_hdfs_obj in client.ls(hdfs_dir):
            if client.isdir(sub_hdfs_obj):
                sub_local_dir = os.path.join(local_dir, sub_hdfs_obj.split("/")[-1])
                GetHDFSOperator._copyDirToDir(sub_hdfs_obj, sub_local_dir, client, file_filter)
            else:
                GetHDFSOperator._copyFileIntoDir(sub_hdfs_obj, local_dir, client, file_filter)

    @staticmethod
    def _copyFileIntoDir(hdfs_file, local_dir, client, file_filter):
        file_name = hdfs_file.split('/')[-1]
        local_path = os.path.join(local_dir, file_name)
        if file_filter is None or file_filter(file_name):
            if not os.path.exists(local_dir):
                os.makedirs(local_dir, mode=0o755)
            client.get(hdfs_path=hdfs_file,
                        local_path=local_path)
            print(f'Get {hdfs_file} to {local_path}')
