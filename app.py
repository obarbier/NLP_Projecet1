# Python libraries
from flask import  Flask, jsonify
from flask_restful import reqparse, abort, Resource, Api
from AskLegalSQLITE import DBSQLITE
app = Flask(__name__)

#initialize SQLDatabase
with DBSQLITE() as askLegal_db:
    askLegal_db.init_db();

api = Api(app)


parser = reqparse.RequestParser()
parser.add_argument('text')

# Util Python File
class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

from rasa_nlu.model import Interpreter

# Switch-Case for Intent section
# define the function blocks
def greet(entity):
    # r = requests.post(url = URL+'/ChatBotWelcome', json = PARAMS)
    return "Hello World"

def thankyou(entity):
    return "Thank you Have a Nice Day"

def legalContact(entity):
    r = Neo4j.legalContactIntentDAO(ext_entity=entity)
    return r
def financialInfo(entity):
    r = Neo4j.financialInfoIntentDAO(ext_entity=entity)
    return r

switcher = {
'greet' : greet,
'thankyou' : thankyou,
'legalContact' : legalContact,
'financialInfo': financialInfo
}
# map the inputs to the function blocks
def Take_Action(argument, entity):
    # Get the function from switcher dictionary
    func = switcher.get(argument)
    # Execute the function
    return func(entity)



# map the inputs to the function blocks
from cypher_impl import LegalBotDataExtract
Neo4j = LegalBotDataExtract(uri = "bolt://localhost:7687", user = "neo4j", password ="pwd")

class HelloWorld(Resource):
    def get(self):
           # user text,
           # nodes TEXT,
           # entity TEXT,
           # where_clause TEXT,
           # created_date INTEGER
        with DBSQLITE() as askLegal_db:
            askLegal_db.query_db('INSERT INTO askLegalTrackerTable(user, nodes, entity,where_clause,created_date)  VALUES (?, ?, ?, ?, ?)',args=['obarbier',':Person','legalContact',"where p.name = 'test'",123])
            query_res = askLegal_db.query_db('SELECT * FROM askLegalTrackerTable')
            for res in query_res:
                print(res['user'])
        return {'hello': 'world'}
    def post(self):
         args= parser.parse_args()
         data_Input_Text = args['text']
         parse_sentence = interpreter.parse(data_Input_Text)
         print(parse_sentence)
         entity = parse_sentence['entities'];
         intent = parse_sentence['intent'];
         return Take_Action(intent['name'],entity);

api.add_resource(HelloWorld, '/')

if __name__ == '__main__':
    from rasa_nlu.training_data import load_data
    from rasa_nlu.model import Trainer
    from rasa_nlu import config
    from rasa_nlu.model import Interpreter
    import os

    training_data = load_data('nlu.md')
    trainer = Trainer(config.load("nlu_config.yml"))

    if os.path.exists("./projects/default/"):
        model_directory = trainer.persist('./projects/default/')  # Returns the directory the model is stored in
        interpreter = Interpreter.load(model_directory)
    else :
        trainer.train(training_data)
        model_directory = trainer.persist('./projects/default/')  # Returns the directory the model is stored in
        interpreter = Interpreter.load(model_directory)

    app.run(host='0.0.0.0', port=80)
