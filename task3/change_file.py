import yaml

def change_file(path, **kwargs):
    date_exe = kwargs['logical_date']
    checkpoints_name = {
        'sales': 'new_sales.yml', 
        'stores': 'new_stores.yml', 
        'inventory': 'new_inventory.yml',
        'products':'new_products.yml'}

    for key, checkpoint in checkpoints_name.items():
        stream = open(path+checkpoint, 'r')

        p = yaml.load(stream, Loader=yaml.FullLoader)
        p['validations'][0]['batch_request']['data_asset_name'] = '{}-{}.csv'.format(date_exe.date(), key)

        with open(path+checkpoint, 'w') as outfile:
            yaml.dump(p, outfile)