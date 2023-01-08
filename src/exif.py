import json

from machina.core.worker import Worker

import exifread

class Exif(Worker):
    types = [
        'png',
        'jpeg',
        'tiff'
    ]

    def __init__(self, *args, **kwargs):
        super(Exif, self).__init__(*args, **kwargs)

    def callback(self, data, properties):
        data = json.loads(data)

        # resolve path
        target = self.get_binary_path(data['ts'], data['hashes']['md5'])
        self.logger.info(f"resolved path: {target}")

        with open(target, 'rb') as f:
            exif_tags = exifread.process_file(f)

        self.logger.info(exif_tags)

        # Convert all values to strings
        tags = {}
        for t in exif_tags.keys():
            tags[t] = str(exif_tags[t])

        # get the appropriate OGM class for the image that was analyzed
        image_cls = self.resolve_db_node_cls(data['type'])
        obj = image_cls.nodes.get(uid=data['uid'])

        # update
        obj.exif = tags
        obj.save()