from neo4j.v1 import GraphDatabase
import jsonpickle
import json

class LegalBotDataExtract(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def legalContactIntentDAO(self , ext_entity):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._legalContactIntentQuery, ext_entity)
            return greeting
    def financialInfoIntentDAO(self , ext_entity):
        with self._driver.session() as session:
            greeting = session.write_transaction(self.financialInfoIntentQuery, ext_entity)
            return greeting
    @staticmethod
    def _legalContactIntentQuery(tx, ext_entity):
        query = "MATCH path = (p:Person)-[:is_legal_contact_for]->(n) Where 1 = 1 "
        for x in ext_entity:
            if x['entity'] == 'PERSON':
                query =  query + 'and p.Name = "' + x['value'] + '"'
            if x['entity'] == 'GPE':
                query =  query + 'and n.Country = "' + x['value']+ '"'
            if x['entity'] == 'ORG':
                query =  query + 'and n.AcctName = "' + x['value']+ '"'
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
    def financialInfoIntentQuery(tx, ext_entity):
        query = "MATCH path = (acct:GES_CustomerAcct)-[:has_finance_data]->(n) Where 1 = 1 "
        for x in ext_entity:
            if x['entity'] == 'PERSON':
                query =  query + 'and p.Name = "' + x['value'] + '"'
            if x['entity'] == 'GPE':
                query =  query + 'and n.Country = "' + x['value']+ '"'
            if x['entity'] == 'ORG':
                query =  query + 'and acct.AcctName = "' + x['value']+ '"'
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
