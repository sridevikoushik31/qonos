# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2014 Rackspace
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import copy
import datetime
import mock
from novaclient import exceptions

from oslo.config import cfg
from qonos.common import exception
from qonos.common import timeutils
from qonos.common import utils as common_utils

import qonos.qonosclient.exception as qonos_ex
from qonos.tests import utils as utils
from qonos.worker.snapshot import snapshot


CONF = cfg.CONF


class BaseTestSnapshotProcessor(utils.BaseTestCase):

    def setUp(self):
        super(BaseTestSnapshotProcessor, self).setUp()
        self.config(image_poll_interval_sec=0.01, group='snapshot_worker')

    def tearDown(self):
        super(BaseTestSnapshotProcessor, self).tearDown()

    def image_fixture(self, image_id, status):
        image = mock.Mock()
        image.id = image_id
        image.status = status
        return image

    def server_instance_fixture(self, name, retention=1):
        server = mock.Mock()
        server.name = name
        server.retention = retention
        return server

    def job_fixture(self):
        now = timeutils.utcnow()
        timeout = now + datetime.timedelta(hours=1)
        hard_timeout = now + datetime.timedelta(hours=4)

        fixture = \
            {
                'id': 'JOB_1',
                'schedule_id': 'SCH_1',
                'tenant': 'TENANT1',
                'worker_id': 'WORKER_1',
                'action': 'snapshot',
                'status': 'QUEUED',
                'timeout': timeout,
                'hard_timeout': hard_timeout,
                'retry_count': 0,
                'metadata':
                {
                    'instance_id': 'INSTANCE_ID',
                    'value': 'my_instance',
                },
            }
        return fixture

    def assert_job_states(self, processor, expected_job_states):
        actual_states = (map(lambda x: x,
                             processor.update_job_calls))
        self.assertEqual(expected_job_states, actual_states)


class TestableSnapshotProcessor(snapshot.SnapshotProcessor):
    def __init__(self, job_fixture, instance_fixture, images_fixture):
        self.job = job_fixture
        self.server = instance_fixture
        self.images = images_fixture
        self.notification_calls = []
        self.org_generate_notification = common_utils.generate_notification
        self.update_job_calls = []
        super(TestableSnapshotProcessor, self).__init__()

    def mock_worker(self):
        self.qonosclient = mock.Mock()

        def update_job(job_id, status, timeout=None, error_message=None):
            self.update_job_calls.append(status)
            return {'job_id': job_id, 'status': status,
                    'timeout': timeout, 'error_message': error_message}

        def update_job_metadata(job_id, metadata):
            return metadata

        def get_qonos_client():
            return self.qonosclient

        worker = mock.Mock()
        worker.update_job = update_job
        worker.update_job_metadata = update_job_metadata
        worker.get_qonos_client = get_qonos_client
        return worker

    def mock_nova_client(self, server, images):
        nova_client = mock.Mock()
        nova_client.servers = mock.Mock()
        if images:
            nova_client.servers.create_image = \
                mock.Mock(mock.ANY, return_value=images[0].id)
        nova_client.servers.get = mock.Mock(mock.ANY, return_value=server)
        nova_client.images = mock.Mock()
        nova_client.images.get = mock.Mock(mock.ANY, side_effect=images)
        return nova_client

    def mock_nova_client_factory(self, job, nova_client):
        nova_client_factory = mock.Mock()
        nova_client_factory.get_nova_client = \
            mock.Mock(job, return_value=nova_client)
        return nova_client_factory

    def mock_notification_calls(self):
        def generate_notification(context, event_type, payload, level='INFO'):
            msg = {'context': context, 'event_type': event_type,
                   'payload': copy.deepcopy(payload), 'level': level}
            self.notification_calls.append(msg)

        common_utils.generate_notification = generate_notification

    def __enter__(self):
        worker = self.mock_worker()
        self.nova_client = self.mock_nova_client(self.server, self.images)
        nova_client_factory = self.mock_nova_client_factory(self.job,
                                                            self.nova_client)
        self.init_processor(worker, nova_client_factory=nova_client_factory)
        self.mock_notification_calls()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        common_utils.generate_notification = self.org_generate_notification


class TestSnapshotProcessorPolling(BaseTestSnapshotProcessor):

    def test_process_job_success_on_first_poll(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.process_job(job)

            self.assertEqual('DONE', job['status'])
            self.assertEqual(0, processor.timeout_count)
            self.assert_job_states(processor, ['PROCESSING', 'DONE'])

    def test_process_job_failure_on_first_poll(self):
        error_states = ['KILLED', 'DELETED', 'PENDING_DELETE', 'ERROR', None]
        for error_status in error_states:
            job = self.job_fixture()
            server = self.server_instance_fixture("test")
            images = [self.image_fixture('IMAGE_ID', error_status)]

            with TestableSnapshotProcessor(job, server, images) as processor:
                self.assertRaises(exception.PollingException,
                                  processor.process_job, job)

                self.assertEqual(0, processor.timeout_count)
                self.assert_job_states(processor, ['PROCESSING', 'ERROR'])

    def test_process_job_retry_polling(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.process_job(job)

            self.assertEqual('DONE', job['status'])
            self.assertEqual(0, processor.timeout_count)
            self.assert_job_states(processor, ['PROCESSING', 'DONE'])

    def test_process_job_image_error_on_any_image_failure_status(self):
        error_states = ['KILLED', 'DELETED', 'PENDING_DELETE', 'ERROR', None]
        for error_status in error_states:
            job = self.job_fixture()
            server = self.server_instance_fixture("test")
            images = [self.image_fixture('IMAGE_ID', 'SAVING'),
                      self.image_fixture('IMAGE_ID', error_status)]

            with TestableSnapshotProcessor(job, server, images) as processor:
                self.assertRaises(exception.PollingException,
                                  processor.process_job, job)

                self.assertEqual(0, processor.timeout_count)
                self.assert_job_states(processor, ['PROCESSING', 'ERROR'])

    def test_process_job_timeout_after_max_retries(self):
        decrement_for_timeout = -10
        self.config(job_timeout_update_increment_min=decrement_for_timeout,
                    group='snapshot_worker')
        self.config(job_timeout_max_updates=3, group='snapshot_worker')

        job = self.job_fixture()
        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'SAVING')]
        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.process_job(job)

            self.assertEqual('TIMED_OUT', job['status'])
            self.assertEqual(3, processor.timeout_count)
            self.assert_job_states(processor, ['PROCESSING',
                                               'PROCESSING',
                                               'PROCESSING',
                                               'PROCESSING',
                                               'TIMED_OUT'])

    def test_process_job_is_successful_after_first_timeout(self):
        decrement_for_timeout = -10
        self.config(job_timeout_update_increment_min=decrement_for_timeout,
                    group='snapshot_worker')
        self.config(job_timeout_max_updates=3, group='snapshot_worker')

        job = self.job_fixture()
        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.process_job(job)

            self.assertEqual('DONE', job['status'])
            self.assertEqual(1, processor.timeout_count)
            self.assert_job_states(processor, ['PROCESSING',
                                               'PROCESSING',
                                               'DONE'])

    def test_process_job_updates_status_when_time_for_update(self):
        decrement_for_next_update = -10
        self.config(job_update_interval_sec=decrement_for_next_update,
                    group='snapshot_worker')

        job = self.job_fixture()
        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.process_job(job)

            self.assertEqual('DONE', job['status'])
            self.assertEqual(0, processor.timeout_count)
            self.assert_job_states(processor, ['PROCESSING',
                                               'PROCESSING',
                                               'PROCESSING',
                                               'DONE'])


class TestSnapshotProcessorJobProcessing(BaseTestSnapshotProcessor):

    def test_process_job_create_image_error(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test")
        images = []

        exc = exceptions.NotFound('Instance not found!!')
        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.nova_client.servers.create_image = \
                mock.Mock(mock.ANY, side_effect=exc)
            processor.process_job(job)
            self.assertEqual('CANCELLED', job['status'])

    def test_process_job_should_cancel_if_schedule_deleted(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            qonosclient = processor.get_qonos_client()
            qonosclient.get_schedule = \
                mock.Mock(side_effect=qonos_ex.NotFound())
            processor.process_job(job)

            self.assertEqual('CANCELLED', job['status'])
            self.assertEqual(0, processor.timeout_count)
            self.assert_job_states(processor, ['CANCELLED'])

    def test_process_job_should_cancel_if_instance_not_found(self):
        job = self.job_fixture()
        images = []

        exp = exceptions.NotFound(404)
        nova_client = mock.Mock()
        nova_client.servers = mock.Mock()

        with TestableSnapshotProcessor(job, None, images) as processor:
            processor.nova_client.servers.get = mock.Mock(mock.ANY,
                                                          side_effect=exp)
            processor.process_job(job)
            self.assertEqual('CANCELLED', job['status'])

    def test_process_job_image_already_exists(self):
        job = self.job_fixture()
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        job['metadata']['image_id'] = images[0].id
        server = self.server_instance_fixture("test")

        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.process_job(job)
            self.assertEqual('DONE', job['status'])
            self.assertEqual(
                processor.nova_client.servers.create_image.call_count, 0)

    def test_process_job_exists_but_had_failed(self):
        job = self.job_fixture()
        images = [self.image_fixture('IMAGE_ID', 'DELETED'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        job['metadata']['image_id'] = images[0].id
        job['metadata']['image_id'] = images[0].id
        server = self.server_instance_fixture("test")

        with TestableSnapshotProcessor(job, server, images) as processor:
            processor.process_job(job)
            self.assertEqual('DONE', job['status'])
            self.assertTrue(processor.nova_client.servers.create_image.called)


class TestSnapshotProcessorRetentionProcessing(BaseTestSnapshotProcessor):

    def test_delete_schedule_positive_retention(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            nova_client = processor.nova_client
            nova_client.rax_scheduled_images_python_novaclient_ext.get = \
                mock.Mock(return_value=server)
            processor._find_scheduled_images_for_server = \
                mock.Mock(return_value=images)
            processor.process_job(job)
            self.assertEqual('DONE', job['status'])
            self.assertTrue(processor.nova_client.images.delete.called)

    def test_delete_schedule_images_greater_than_retention(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            nova_client = processor.nova_client
            nova_client.rax_scheduled_images_python_novaclient_ext.get = \
                mock.Mock(return_value=server)
            processor._find_scheduled_images_for_server = \
                mock.Mock(return_value=images)
            processor.process_job(job)
            self.assertEqual('DONE', job['status'])
            self.assertEqual(2, processor.nova_client.images.delete.call_count)

    def test_delete_schedule_images_less_than_retention(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            nova_client = processor.nova_client
            nova_client.rax_scheduled_images_python_novaclient_ext.get = \
                mock.Mock(return_value=server)
            processor._find_scheduled_images_for_server = \
                mock.Mock(return_value=images)
            processor.process_job(job)
            self.assertEqual('DONE', job['status'])
            self.assertEqual(0, processor.nova_client.images.delete.call_count)

    def test_delete_schedule_bad_retention(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test", retention='blah')
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            qonosclient = processor.get_qonos_client()
            processor.process_job(job)
            self.assertEqual('DONE', job['status'])
            self.assertEqual(0, processor.nova_client.images.delete.call_count)
            self.assertTrue(qonosclient.delete_schedule.called)

    def test_delete_schedule_zero_retention(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test", retention=0)
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE')]

        with TestableSnapshotProcessor(job, server, images) as processor:
            qonosclient = processor.get_qonos_client()
            nova_client = processor.nova_client
            nova_client.rax_scheduled_images_python_novaclient_ext.get = \
                mock.Mock(return_value=server)
            processor._find_scheduled_images_for_server = \
                mock.Mock(return_value=images)
            processor.process_job(job)
            self.assertEqual('DONE', job['status'])
            self.assertEqual(0, processor.nova_client.images.delete.call_count)
            self.assertTrue(qonosclient.delete_schedule.called)


class TestSnapshotProcessorImageName(BaseTestSnapshotProcessor):

    def test_generate_image_name(self):
        job = self.job_fixture()
        timeutils.set_time_override(datetime.datetime(2013, 3, 22, 22, 39, 27))
        timestamp = '1363991967'

        with TestableSnapshotProcessor(job, None, []) as processor:
            image_name = processor.generate_image_name("test")
            self.assertEqual(image_name, 'Daily-test-' + timestamp)

    def test_generate_image_name_long_server_name(self):
        job = self.job_fixture()

        timeutils.set_time_override(datetime.datetime(2013, 3, 22, 22, 39, 27))
        timestamp = '1363991967'
        fake_server_name = 'a' * 255
        expected_server_name = 'a' * (255 - len(timestamp) - len('Daily--'))

        with TestableSnapshotProcessor(job, None, []) as processor:
            image_name = processor.generate_image_name(fake_server_name)
            expected_image_name = \
                'Daily-' + expected_server_name + '-' + timestamp
            self.assertEqual(image_name, expected_image_name)
