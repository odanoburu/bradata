import bradata.utils
import bradata.connection


import io
from zipfile import ZipFile
import pandas as pd
import glob
import requests
import yaml

import luigi
import luigi.contrib.postgres

class Get_Headers(luigi.Task):

    def output(self):
        return luigi.LocalTarget(bradata.utils._set_download_directory() + '/tse/config/headers.csv')

    def run(self):
        conn = bradata.connection.Connection()

        result = conn.perform_request('https://raw.githubusercontent.com/labFGV/bradata/master/bradata/tse/headersTSE.csv')

        if result['status'] == 'ok':
            result = result['content']
        else:
            print('File was not dowloaded')

        with self.output().open('w') as o_file:
            o_file.write(result)


class Get_Header_Relation(luigi.Task):
    def output(self):
        return luigi.LocalTarget(bradata.utils._set_download_directory() + '/tse/config/header_relation.yaml')

    def run(self):
        conn = bradata.connection.Connection()

        result = conn.perform_request(
            'https://raw.githubusercontent.com/labFGV/bradata/master/bradata/tse/header_relation.yaml')

        if result['status'] == 'ok':
            result = result['content']
        else:
            print('File was not dowloaded')

        with self.output().open('w') as o_file:
            o_file.write(result)


class Get_URL(luigi.Task):
    """
    """

    def output(self):
        return luigi.LocalTarget(bradata.utils._set_download_directory() + '/tse/config/url_relation.yaml')

    def run(self):
        conn = bradata.connection.Connection()

        result = conn.perform_request(
            'https://raw.githubusercontent.com/labFGV/bradata/master/bradata/tse/url_relation.yaml')

        if result['status'] == 'ok':
            result = result['content']
        else:
            print('File was not dowloaded')

        with self.output().open('w') as o_file:
            o_file.write(result)


class Download_Unzip(luigi.Task):
    """
    """

    year = luigi.Parameter()
    tipo = luigi.Parameter()

    def output(self):
        """
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(bradata.utils._set_download_directory() +
                                 '/tse/temp/{}_{}/'.format(self.tipo, self.year))

    def requires(self):
        """
        * :py:class:`~.Streams`
        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return Get_URL()

    def run(self):
        conn = bradata.connection.Connection()


        with self.input().open('r') as input_file:
            base_url = self.select_url(self.tipo)

            url = base_url + bradata.utils._treat_inputs(self.year) + '.zip'

            result = conn.perform_request(url, binary=True)

            if result['status'] == 'ok':
                result = result['content']
            else:
                print('File was not dowloaded')

            zipfile = ZipFile(io.BytesIO(result))

            zipfile.extractall(self.output().path)

    def select_url(self, tipo):

        with open(self.input().path, 'r') as f:
            data = yaml.load(f)

        return data[tipo]


class Aggregat(luigi.Task):
    """
    """

    year = luigi.Parameter()
    tipo = luigi.Parameter()

    def requires(self):
        """
        * :py:class:`~.AggregateArtists` or
        * :py:class:`~.AggregateArtistsHadoop` if :py:attr:`~/.Top10Artists.use_hadoop` is set.
        :return: object (:py:class:`luigi.task.Task`)
        """

        return {'download': Download_Unzip(tipo=self.tipo, year=self.year),
                'headers': Get_Headers(),
                'header_relation': Get_Header_Relation()}

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(bradata.utils._set_download_directory() +
                                 '/tse/{}_{}.csv'.format(self.tipo, self.year))

    def run(self):

        headers = pd.read_csv(self.input()['headers'].path)

        files = glob.glob(self.input()['download'].path + "*.txt".format(self.year))

        header = self.find_header(self.tipo, self.year)

        df_list = []
        for filename in sorted(files):
            df_list.append(
                pd.read_csv(filename, sep=';', names=headers[header].dropna().tolist(), encoding='latin1'))


        full_df = pd.concat(df_list)

        full_df.to_csv(self.output().path, index=False)

    def find_header(self, tipo, ano):
        with open(self.input()['header_relation'].path, 'r') as f:
            data = yaml.load(f)
        print(data)
        a = data[tipo]

        final = min(data['candidatos'].keys())
        for k in a.keys():
            if int(ano) >= k:
                final = k
        return str(a[final])


if __name__ == "__main__":
    luigi.run()