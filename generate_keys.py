import secrets
import json

KEY_SIZE = 16
NUMBER_OF_KEYS = 50

with open('test-config-small.json', 'r') as config:
    with open('init.csv', 'w') as init:
        config_data = json.load(config)
        keys = []
        for i in range(NUMBER_OF_KEYS):
            key = "0"
            while key[0].isdigit() or key in keys:
                key = secrets.token_hex(KEY_SIZE // 2)
            keys.append(key)
            init.write(f'{key},0\n')
        config_data["keys"] = ' '.join(keys)
        with open('test-config.json', 'w') as f:
            json.dump(config_data, f, indent=4)
