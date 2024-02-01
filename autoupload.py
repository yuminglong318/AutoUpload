from motor import motor_asyncio
from config import *
import asyncio
import re

client = motor_asyncio.AsyncIOMotorClient(MONGODB_URL, tlsAllowInvalidCertificates=True)
db = client[DATABASE]
collection = db[COLLECTION]

def get_username_from_url(url, social):
    if social == 'instagram':
        pattern = re.compile(r'^https?://(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?$')
    elif social == 'linkedin':
        pattern = re.compile(r'^https?://(?:www\.)?linkedin.com/in/([A-Za-z0-9_.%-]+)/?$')
    elif social == 'tiktok':
        return url
    elif social == 'facebook':
        pattern = re.compile(r'^https?://(?:m.|www\.)?facebook\.com/([A-Za-z0-9_.]+)/?$')
    else:
        return None
    match = pattern.findall(url)
    return match[0] if match else None

async def get_student(first_name, last_name, university):
    
    filter_options = {
        'first_name': first_name,
        'last_name': last_name,
        'university': university
    }
    sort_option = [('_id', -1)]

    document = await collection.find_one(filter_options, sort=sort_option)

    return document

async def update_student(document, social):
    
    for key in social.keys():
        
        if len(social[key]) == 0:
            continue

        if (key not in [sc['name'] for sc in document['social']]):
            document['social'].append({
                'name': key,
                'userName': get_username_from_url(social[key][0], key),
                'followers': None,
                'image': '',
                'bio': ''
            })
        
        for sc in document['social']:
            if sc['name'] == 'tiktok' and (not sc['userName'].startswith("https")):
                sc['userName'] = f"https://www.tiktok.com/@{sc['userName']}"
    
    update = {'$set': {'social': document['social']}}
    await collection.update_one({'_id': document['_id']}, update)

async def add_student(student):
    social = []
    followers = None
    for key, value in student.get('social').items():
        if len(value) == 0:
            continue
        try:
            social.append({
                'name': key,
                'userName': value[0].get('username'),
                'followers': value[0].get('followers'),
                'bio': value[0].get('bio'),
                'image': value[0].get('instagram picture')
            })
            followers = value[0].get('followers')
        except Exception as e:
            social.append({
                'name': key,
                'userName': get_username_from_url(value[0], key),
                'followers': None,
                'bio': '',
                'image': ''
            })

    document = {
        'first_name': student.get('first_name'),
        'last_name': student.get('last_name'),
        'typeofContact': ['Students', 'last_name'],
        'image': student.get('profile picture'),
        'school': student.get('school'),
        'university': student.get('university'),
        'social': social,
        'followers': followers,
        'verified': True,
        'organization': False,
    }
    
    await collection.insert_one(document)

if __name__ == '__main__':
    import json
    import os

    directory = 'data'
    for file in os.listdir(directory):
        print(file)
        file_path = os.path.join(directory, file)

        with open(file_path, 'r', encoding= 'utf-8') as f:
            student_data = json.load(f)

        for stu in student_data:
            loop = asyncio.get_event_loop()
            student = loop.run_until_complete(get_student(stu['first_name'], stu['last_name'], stu['university']))
            if student:
                try:
                    loop.run_until_complete(update_student(student, stu['social']))
                except Exception as e:
                    print("Update Error: ", stu['first_name'], stu['last_name'], stu['university'])
            else:
                print('Not Found', stu['first_name'], stu['last_name'], 'From', stu['university'])
                loop.run_until_complete(add_student(stu))
                

        