import pandas as pd
data_List_Json =[{'user_id': 1, 'login': 'user1', 'gravatar_id': 'gravatar1', 'avatar_url': 'https://avatar.com/user1', 'url': 'https://api.user1.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:19:22.119000'},
    {'user_id': 2, 'login': 'user2', 'gravatar_id': 'gravatar2', 'avatar_url': 'https://avatar.com/user2', 'url': 'https://api.user2.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.592000'},
    {'user_id': 3, 'login': 'user3', 'gravatar_id': 'gravatar3', 'avatar_url': 'https://avatar.com/user3', 'url': None, 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.602000'},
    {'user_id': 4, 'login': 'user4', 'gravatar_id': 'gravatar4', 'avatar_url': None, 'url': None, 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.611000'},
    {'user_id': 5, 'login': 'user5', 'gravatar_id': None, 'avatar_url': 'https://avatar.com/user5', 'url': 'https://api.user5.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.621000'},
    {'user_id': 6, 'login': 'user6', 'gravatar_id': None, 'avatar_url': None, 'url': None, 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.628000'},
    {'user_id': 7, 'login': 'user7', 'gravatar_id': 'gravatar7', 'avatar_url': 'https://avatar.com/user7', 'url': 'https://api.user7.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.636000'},
    {'user_id': 8, 'login': 'user8', 'gravatar_id': None, 'avatar_url': 'https://avatar.com/user8', 'url': None, 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.645000'},
    {'user_id': 9, 'login': 'user9', 'gravatar_id': 'gravatar9', 'avatar_url': None, 'url': 'https://api.user9.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.654000'},
    {'user_id': 10, 'login': 'user10', 'gravatar_id': 'gravatar10', 'avatar_url': 'https://avatar.com/user10', 'url': 'https://api.user10.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.663000'}
]



data_producer = [{'user_id': 1, 'login': 'user1', 'gravatar_id': 'gravatar1', 'avatar_url': 'https://avatar.com/user1', 'url': 'https://api.user1.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:19:22.119000'}]
#     -
data_consumer = [{"user_id": 1, "login": "user1", "gravatar_id": "gravatar1", "avatar_url": "https://avatar.com/user1", "url": "https://api.user1.com", "state": "INSERT", "log_timestamp": "2025-07-24 15:19:22.119000"}
                 ,{'user_id': 2, 'login': 'user2', 'gravatar_id': 'gravatar2', 'avatar_url': 'https://avatar.com/user2', 'url': 'https://api.user2.com', 'state': 'INSERT', 'log_timestamp': '2025-07-24 15:27:35.592000'}]
#     !=
#     0
# =>>> missing records => phục hồi record thiếu
# =>>> không bị missing records


difference = [item for item in data_List_Json if item not in data_consumer]

for i in difference:
    print(i)

