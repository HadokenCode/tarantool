
space = box.schema.space.create('test', { engine = 'phia' })
index = space:create_index('primary', { type = 'tree', parts = {1, 'str'} })
box.phia['phia.version']
space:drop()
