import requests

from stacktach import utils as stackutils
from stacktach.reconciler import exceptions
from stacktach.reconciler.utils import empty_reconciler_instance

GET_INSTANCE_QUERY = "SELECT * FROM instances where uuid ='%s';"

METADATA_MAPPING = {
    'image_org.openstack__1__architecture': 'os_architecture',
    'image_org.openstack__1__os_distro': 'os_distro',
    'image_org.openstack__1__os_version': 'os_version',
    'image_com.rackspace__1__options': 'rax_options',
}
METADATA_FIELDS = ["'%s'" % x for x in METADATA_MAPPING.keys()]
METADATA_FIELDS = ','.join(METADATA_FIELDS)

GET_INSTANCE_SYSTEM_METADATA = """
SELECT * FROM instance_system_metadata
    WHERE instance_uuid = '%s' AND
    deleted = 0 AND `key` IN (%s);
"""
GET_INSTANCE_SYSTEM_METADATA %= ('%s', METADATA_FIELDS)

DELETED_VM_STATE = 'deleted'


def _json(result):
    if callable(result.json):
        return result.json()
    else:
        return result.json


class JSONBridgeClient(object):
    src_str = 'json_bridge:nova_db'

    def __init__(self, config):
        self.config = config

    def _url_for_region(self, region):
        return self.config['url'] + self.config['databases'][region]

    def _do_query(self, region, query):
        data = {'sql': query}
        credentials = (self.config['username'], self.config['password'])
        return _json(requests.post(self._url_for_region(region), data,
                                   verify=False, auth=credentials))

    def _to_reconciler_instance(self, instance, metadata=None):
        r_instance = empty_reconciler_instance()
        r_instance.update({
            'id': instance['uuid'],
            'tenant': instance['project_id'],
            'instance_type_id': str(instance['instance_type_id']),
        })

        if instance['launched_at'] is not None:
            launched_at = stackutils.str_time_to_unix(instance['launched_at'])
            r_instance['launched_at'] = launched_at

        if instance['terminated_at'] is not None:
            deleted_at = stackutils.str_time_to_unix(instance['terminated_at'])
            r_instance['deleted_at'] = deleted_at

        if instance['vm_state'] == DELETED_VM_STATE:
            r_instance['deleted'] = True

        if metadata is not None:
            r_instance.update(metadata)

        return r_instance

    def _get_instance_meta(self, region, uuid):
        results = self._do_query(region, GET_INSTANCE_SYSTEM_METADATA % uuid)
        metadata = {}
        for result in results['result']:
            key = result['key']
            if key in METADATA_MAPPING:
                metadata[METADATA_MAPPING[key]] = result['value']
        return metadata

    def get_instance(self, region, uuid, get_metadata=False):
        results = self._do_query(region, GET_INSTANCE_QUERY % uuid)['result']
        if len(results) > 0:
            metadata = None
            if get_metadata:
                metadata = self._get_instance_meta(region, uuid)
            return self._to_reconciler_instance(results[0], metadata=metadata)
        else:
            msg = "Couldn't find instance (%s) using JSON Bridge in region (%s)"
            raise exceptions.NotFound(msg % (uuid, region))
