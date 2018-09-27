from neo4j.v1 import GraphDatabase
from AskLegalSQLITE import DBSQLITE
import datetime
import jsonpickle
import json

class LegalBotDataExtract(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def legalContactIntentDAO(self , ext_entity):
        with self._driver.session() as session:
            dao = session.write_transaction(self._legalContactIntentQuery, ext_entity)
            return dao
    def financialInfoIntentDAO(self , ext_entity):
        with self._driver.session() as session:
            dao = session.write_transaction(self._financialInfoIntentQuery, ext_entity)
            return dao
    def defaultDAO(self , ext_entity):
        with self._driver.session() as session:
            dao = session.write_transaction(self._defaultQuery, ext_entity)
            return dao
    @staticmethod
    def _legalContactIntentQuery(tx, ext_entity):
        if not ext_entity:
            query = "MATCH path = (p:Person)-[*]-" # because I know data I am using this query But may need to change
            with DBSQLITE() as askLegal_db:
                rows = askLegal_db.getUserInfo()
                if rows:
                    query =  query + rows[0]['nodes']
                else:
                    query = query + '()'
                print(query)
        else:
            query = "MATCH path = (p:Person)-[:is_legal_contact_for]->(n) Where 1 = 1 "
            for x in ext_entity:
                if x['entity'] == 'PERSON':
                    node = "(:Person {Name : '" + x['value'] +"'})"
                    where_clause = 'and Person.Name = "' + x['value'] + '"'
                    queryClause  = 'and p.Name = "' + x['value'] + '"'
                if x['entity'] == 'GPE':
                    node = "(:Country {Country : '" + x['value'] + "'})"
                    where_clause = 'and Country.Country = "' + x['value']+ '"'
                    queryClause  = 'and n.Country = "' + x['value']+ '"'
                if x['entity'] == 'ORG':
                    node = "(:GES_CustomerAcct {AcctName : '" + x['value'] + "'})"
                    where_clause = 'and GES_CustomerAcct.AcctName = "' + x['value']+ '"'
                    queryClause  = 'and n.AcctName = "' + x['value']+ '"'
                query =  query + queryClause
                dbArgs = [];
                dbArgs.append('obarbier')
                dbArgs.append(node)
                dbArgs.append(x['entity'])
                dbArgs.append(where_clause)
                dbArgs.append(str(datetime.datetime.now()))
                with DBSQLITE() as askLegal_db:
                    askLegal_db.logUserInfo(args = dbArgs)
        query = query + 'RETURN path LIMIT 5';
        print(query)
        results = tx.run(query)
        recs = results.records() #return list of Records
        output = []
        for rec in recs: #get each record in the form or [key,vals] from all records
            keys = rec.keys() # return record dataType
            vals = rec.values()
            for idx in range(len(keys)): # get all nodes in a one record, len(keys) return the length of a record
                nodes = vals[idx].nodes
                temp_array = []
                for node in nodes: #in each node do somthing
                    temp_dict = {}
                    if(node.get('Name') != None):
                        temp_dict['entityType']='Person';
                        temp_dict['entity']= node.get('Name');
                    if(node.get('AcctName') != None):
                        temp_dict['entityType']='Account';
                        temp_dict['entity']= node.get('AcctName');
                    if(node.get('Country') != None):
                        temp_dict['entityType']='Country';
                        temp_dict['entity']= node.get('Country');
                    temp_array.append(temp_dict)
                output.append(temp_array)

        return output


    @staticmethod
    def _financialInfoIntentQuery(tx, ext_entity):
        if not ext_entity:
            query = "MATCH path = (f:finances)<-[*]-"# because I know data I am using this query But may need to change
            with DBSQLITE() as askLegal_db:
                rows = askLegal_db.getUserInfo()
                if rows:
                    query =  query + rows[0]['nodes']
                else:
                    query = query + '()'
                print(query)
        else:
            query = "MATCH path = (acct:GES_CustomerAcct)-[:has_finance_data]->(n) Where 1 = 1 "
            for x in ext_entity:
                if x['entity'] == 'PERSON':
                    node = "(:Person {Name : '" + x['value'] + "'})"
                    where_clause = 'and Person.Name = "' + x['value'] + '"'
                    queryClause  = 'and n.Name = "' + x['value'] + '"'
                if x['entity'] == 'GPE':
                    node = "(:Country {Country : '" + x['value'] + "'})"
                    where_clause = 'and Country.Country = "' + x['value']+ '"'
                    queryClause  = 'and n.Country = "' + x['value']+ '"'
                if x['entity'] == 'ORG':
                    node = "(:GES_CustomerAcct {AcctName : '" + x['value'] + "'})"
                    where_clause = 'and GES_CustomerAcct.AcctName = "' + x['value']+ '"'
                    queryClause  = 'and acct.AcctName = "' + x['value']+ '"'
                query =  query + queryClause
                dbArgs = [];
                dbArgs.append('obarbier')
                dbArgs.append(node)
                dbArgs.append(x['entity'])
                dbArgs.append(where_clause)
                dbArgs.append(str(datetime.datetime.now()))
                with DBSQLITE() as askLegal_db:
                    askLegal_db.logUserInfo(args = dbArgs)
        query = query + 'RETURN path LIMIT 5';
        print(query)
        results = tx.run(query)
        recs = results.records() #return list of Records
        output = []
        for rec in recs: #get each record in the form or [key,vals] from all records
            keys = rec.keys() # return record dataType
            vals = rec.values()
            for idx in range(len(keys)): # get all nodes in a one record, len(keys) return the length of a record
                nodes = vals[idx].nodes
                temp_array = []
                for node in nodes: #in each node do somthing
                    temp_dict = {}
                    if(node.get('Name') != None):
                        temp_dict['entityType']='Person';
                        temp_dict['entity']= node.get('Name');
                    if(node.get('AcctName') != None):
                        temp_dict['entityType']='Account';
                        temp_dict['entity']= node.get('AcctName');
                    if(node.get('Country') != None):
                        temp_dict['entityType']='Country';
                        temp_dict['entity']= node.get('Country');
                    if(node.get('AverageBooking') != None):
                        temp_dict['entityType']='financesData';
                        temp_dict['entity']= node.get('AverageBooking');

                    temp_array.append(temp_dict)
                output.append(temp_array)

        return output
    @staticmethod
    def _defaultQuery(tx, ext_entity):
        lengthOf_ext_entity= len(ext_entity)
        if ext_entity[0]['entity'] == 'PERSON':
            node = "(:Person {Name : '" + x['value'] + "'})"
        if ext_entity[0]['entity'] == 'GPE':
            node = "(:Country {Country : '" + x['value'] + "'})"
        if ext_entity[0]['entity'] == 'ORG':
            node = "(:GES_CustomerAcct {AcctName : '" + x['value'] + "'})"
        dbArgs = [];
        dbArgs.append('obarbier') # will need to add cec id
        dbArgs.append(node)
        dbArgs.append(x['entity'])
        dbArgs.append(where_clause)
        dbArgs.append(str(datetime.datetime.now()))
        with DBSQLITE() as askLegal_db:
            askLegal_db.logUserInfo(args = dbArgs)
        query = 'MATCH path = ' + node
        if lengthOf_ext_entity > 1:
            for idx in range(1,lengthOf_ext_entity):
                if ext_entity[idx]['entity'] == 'PERSON':
                    node = "(:Person {Name : '" +  ext_entity[idx]['value'] + "'})"
                if ext_entity[idx]['entity'] == 'GPE':
                    node = "(:Country {Country : '" +  ext_entity[idx]['value'] + "'})"
                if ext_entity[idx]['entity'] == 'ORG':
                    node = "(:GES_CustomerAcct {AcctName : '" +  ext_entity[idx]['value'] + "'})"
                query =  query + '-[*]-' + node
                dbArgs = [];
                dbArgs.append('obarbier') # will need to add cec id
                dbArgs.append(node)
                dbArgs.append(x['entity'])
                dbArgs.append(where_clause)
                dbArgs.append(str(datetime.datetime.now()))
                with DBSQLITE() as askLegal_db:
                    askLegal_db.logUserInfo(args = dbArgs)
            query = query + 'RETURN path LIMIT 5';
        else:
            query = query +  '-[*1..5]-' +'()'
        print(query)
        results = tx.run(query)
        recs = results.records() #return list of Records
        output = []
        for rec in recs: #get each record in the form or [key,vals] from all records
            keys = rec.keys() # return record dataType
            vals = rec.values()
            for idx in range(len(keys)): # get all nodes in a one record, len(keys) return the length of a record
                nodes = vals[idx].nodes
                temp_array = []
                for node in nodes: #in each node do somthing
                    temp_dict = {}
                    if(node.get('Name') != None):
                        temp_dict['entityType']='Person';
                        temp_dict['entity']= node.get('Name');
                    if(node.get('AcctName') != None):
                        temp_dict['entityType']='Account';
                        temp_dict['entity']= node.get('AcctName');
                    if(node.get('Country') != None):
                        temp_dict['entityType']='Country';
                        temp_dict['entity']= node.get('Country');
                    if(node.get('AverageBooking') != None):
                        temp_dict['entityType']='financesData';
                        temp_dict['entity']= node.get('AverageBooking');

                    temp_array.append(temp_dict)
                output.append(temp_array)

        return output
