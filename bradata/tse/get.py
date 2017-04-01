import os

def args(tipo:None, year:None):

    if not isinstance(tipo, list):
        tipo = [tipo]
    if not isinstance(year, list):
        year = [year]

    for t in tipo:
        for y in year:
            os.system("PYTHONPATH='.' luigi --module bradata.tse.pipeline"
                      " Aggregat --local-scheduler --tipo {} --year {}".format(t, y))