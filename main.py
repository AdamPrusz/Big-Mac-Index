import quandl
import csv
import pandas as pd
import boto3

class Data:
    def __init__(self, countries_file, key):
        self.countries_file = countries_file
        self.key = key
           
    # get the list of country names and codes, using the file downloaded from NASDAQ
    def list_of_countries(self):    
        country_names_codes = []

        with open(self.countries_file, 'r') as file:
            csv_reader = csv.reader(file)

            for row in csv_reader:
                country = row[0].split('|')
                country_names_codes.append(country)

        return country_names_codes[1:]

    # function returning Big Mac data for one country (type: Pandas DataFrame)
    def get_big_mac_data(self, country_code):
        quandl.ApiConfig.api_key = self.key
        one_country_big_mac = quandl.get(f'ECONOMIST/BIGMAC_{country_code}')

        return one_country_big_mac

    # function returning Big Mac data for all countries
    def all_countries(self, get_function, countries):   
        list_of_big_mac_data = []

        for country in countries:  
            country_name = country[0]
            country_code = country[1] 

            one_country_data = get_function(self.key, country_code)
            one_country_data['country'] = country_name

            list_of_big_mac_data.append(one_country_data)


        all_countries_big_mac = pd.concat(list_of_big_mac_data)

        # Removing NAN values  
        all_countries_big_mac.loc[:, 'dollar_adj_valuation': 'yuan_adj_valuation'] = \
        all_countries_big_mac.loc[:, 'dollar_adj_valuation': 'yuan_adj_valuation'].fillna(0)

        return all_countries_big_mac

    # saving data frame with all countries as the csv file
    def save_to_csv(self, big_mac_data, filename): 
        try:
            big_mac_data.to_csv(f'{filename}.csv')
        except PermissionError:
            print("Close the spreadsheet and try again.")
            
    def save_first_five_countries(self, big_mac_data):   
        # move index 'data' to column
        big_mac_data = big_mac_data.reset_index()
        
        # set the date equals to 31/07/2021
        july_2021 = big_mac_data[big_mac_data['Date'] == '2021-07-31']
        
        # Sorting by values and get first 5 top scores
        july_2021 = july_2021.sort_values(by = 'dollar_adj_valuation', ascending = False).head(5)     
      
        # saving data frame with first 5 countries as the csv file
        try:
            july_2021.to_csv('top_five_big_mac_july_2021.csv')
            return 
        except PermissionError:
            print("Close the spreadsheet and try again.")   
            
class AWS:
    def __init__(self, key_id, secret_key, region):
        self.key_id = key_id
        self.secret_key = secret_key
        self.region = region
        
    def create_client(self, client_type):
        client = boto3.client(client_type,
                          region_name = self.region,
                          aws_access_key_id = self.key_id,
                       aws_secret_access_key = self.secret_key)  
        print(f"Client {client_type} ahs been created!")
        return client
    
    
    #create bucket on s3
    def create_bucket(self, s3_client, bucket_name):
        try:
            bucket = s3_client.create_bucket(Bucket = bucket_name, CreateBucketConfiguration={'LocationConstraint': self.region})
            print(f"Bucket {bucket_name} has been created!")
            return bucket
        except Exception as err:
            return f"Something went wrong. Type error: {err}"
    
    
    #load the file into the created bucket on S3
    def load_file_to_s3(self, s3_client, file_path, file_key, bucket_name):
        try:
            s3_client.upload_file(Filename = file_path,
                           Key = file_key,
                           Bucket = bucket_name,
                           ExtraArgs = {'ACL': 'public-read'})
            return f"File {file_path} has been loaded on S3!"
        except Exception as err:
            return f"Something went wrong. Type error: {err}"
    
    
    #create topic, add subscribers to the topic and send notifiction
    def create_topic(self, sns_client, topic_name):
        try:
            topic_id = sns_client.create_topic(Name = topic_name)['TopicArn']
            
            print(f"Topic {topic_name} has been created!")
            return topic_id
        except Exception as err:
            return f"Something went wrong. Type error: {err}"
        
        
    def add_new_subscriber_to_the_topic(self, sns_client, topic, protocol, email_or_phone_number):
        try:
            new_subs = sns_client.subscribe(
                TopicArn = topic,
                Protocol = protocol,
                Endpoint = email_or_phone_number)
            return f"New subscriber {email_or_phone_number} has been add to the topic: {topic}"
        except Exception as err:
            return f"Something went wrong. Type error: {err}"
        
        
    def send_notification(self, sns_client, topic, message, subject):
        try:
            sns_client.publish(
                TopicArn = topic,
                Message = message,
                Subject = subject)
            return "Notification has been sent to all the subscribers"
        except Exception as err:
            return f"Something went wrong. Type error: {err}"
            
            
#-----results------------------------------------------------------------------------------------

file_name = r'C:\Users\adamp\Downloads\economist_country_codes.csv'
API_key = input("Please provide your API key to NASDAQ: ")

aws_region = 'eu-central-1'
aws_key = input("Please provide your AWS key: ")
aws_secret_key = input("Please provide your AWS secret key: ")

file = r'C:\Users\adamp\Documents\Onwelo zadania\all_big_mac_july_2021.csv'
key_file =  r'C:\Users\adamp\Documents\Onwelo zadania\all_big_mac_july_2021.csv'
bucket_name = "test-bucket-created-by-prusz2"
topic_name = "test-topic-created-by-prusz2"


my_data = Data(file_name, API_key)
my_aws = AWS(aws_key, aws_secret_key, aws_region)

all_countries_codes_names = my_data.list_of_countries()
all_countries_big_mac_data =  my_data.all_countries(get_big_mac_data, all_countries_codes_names)

my_data.save_to_csv(all_countries_big_mac_data, 'all_big_mac_july_2021')
my_data.save_first_five_countries(all_countries_big_mac_data)

client_s3 = my_aws.create_client('s3')
client_sns = my_aws.create_client('sns')

bucket = my_aws.create_bucket(client_s3, bucket_name)

loaded_file = my_aws.load_file_to_s3(client_s3, file, key_file, bucket_name)
print(loaded_file)

topic = my_aws.create_topic(client_sns, topic_name)
print(topic)

print(my_aws.add_new_subscriber_to_the_topic(client_sns, topic, 'email', 'adampruszynski95@gmail.com'))
print(my_aws.send_notification(client_sns, topic, "The file has been loaded on S3", "AWS notification"))
