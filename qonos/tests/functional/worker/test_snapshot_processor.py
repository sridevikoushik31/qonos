# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2013 Rackspace
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

import datetime
import mock
from novaclient import exceptions

from oslo.config import cfg
from qonos.common import exception
from qonos.common import timeutils

from qonos.tests import utils as utils
from qonos.worker.snapshot import snapshot


CONF = cfg.CONF


class TestSnapshotProcessor(utils.BaseTestCase):

    def setUp(self):
        super(TestSnapshotProcessor, self).setUp()
        self.config(image_poll_interval_sec=0.01, group='snapshot_worker')

    def tearDown(self):
        super(TestSnapshotProcessor, self).tearDown()

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

    def mock_worker(self):
        worker = mock.Mock()
        self.qonosclient = mock.Mock()

        def update_job(job_id, status, timeout=None, error_message=None):
            return {'job_id': job_id, 'status': status,
                    'timeout': timeout, 'error_message': error_message}

        def update_job_metadata(job_id, metadata):
            return metadata

        def get_qonos_client():
            return self.qonosclient

        worker.update_job = update_job
        worker.update_job_metadata = update_job_metadata
        worker.get_qonos_client = get_qonos_client

        return worker

    def mock_nova_client(self, server, images):
        nova_client = mock.Mock()
        nova_client.servers = mock.Mock()
        nova_client.servers.create_image = mock.Mock(mock.ANY,
                                                     return_value=images[0].id)
        nova_client.servers.get = mock.Mock(mock.ANY, return_value=server)
        nova_client.images = mock.Mock()
        nova_client.images.get = mock.Mock(mock.ANY, side_effect=images)
        return nova_client

    def mock_nova_client_factory(self, job, nova_client):
        nova_client_factory = mock.Mock()
        nova_client_factory.get_nova_client = \
            mock.Mock(job, return_value=nova_client)
        return nova_client_factory

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

    def test_successfully_processed_job(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])

    def test_process_job_image_error(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ERROR')]
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        self.assertRaises(exception.PollingException,
                          processor.process_job, job)

    def test_process_job_image_killed(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'KILLED')]
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        self.assertRaises(exception.PollingException,
                          processor.process_job, job)

    def test_process_job_image_deleted(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'DELETED')]
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        self.assertRaises(exception.PollingException,
                          processor.process_job, job)

    def test_process_job_image_pending_delete(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'PENDING_DELETE')]
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        self.assertRaises(exception.PollingException,
                          processor.process_job, job)

    def test_process_job_image_status_none(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', None)]
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        self.assertRaises(exception.PollingException,
                          processor.process_job, job)

    def test_process_job_create_image_error(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        exc = exception.NotFound('Instance not found!!')
        server = self.server_instance_fixture("test")
        nova_client = mock.Mock()
        nova_client.servers = mock.Mock()
        nova_client.servers.create_image = mock.Mock(mock.ANY,
                                                     side_effect=exc)

        nova_client.servers.get = mock.Mock(mock.ANY, return_value=server)
        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        self.assertRaises(exception.NotFound, processor.process_job, job)

    def test_process_job_should_cancel_if_schedule_deleted(self):
        job = self.job_fixture()
        job['metadata']['instance_id'] = None
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        images = [self.image_fixture('IMAGE_ID', 'QUEUED')]
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        self.assertEqual('CANCELLED', job['status'])

    def test_process_job_should_cancel_if_instance_not_found(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        exp = exceptions.NotFound(404)
        nova_client = mock.Mock()
        nova_client.servers = mock.Mock()
        nova_client.servers.get = mock.Mock(mock.ANY,
                                            side_effect=exp)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        self.assertEqual('CANCELLED', job['status'])

    def test_process_job_image_exists(self):
        job = self.job_fixture()
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        job['metadata']['image_id'] = images[0].id
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])
        self.assertEqual(nova_client.servers.create_image.call_count, 0)

    def test_process_job_exists_status_failed(self):
        job = self.job_fixture()
        images = [self.image_fixture('IMAGE_ID', 'DELETED'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        job['metadata']['image_id'] = images[0].id
        worker = self.mock_worker()

        server = self.server_instance_fixture("test")
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])
        self.assertTrue(nova_client.servers.create_image.called)

    def test_process_job_status_only_update(self):
        job = self.job_fixture()
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        returns = [{'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'DONE',
                    'timeout': None, 'error_message': None}]
        worker = self.mock_worker()
        worker.update_job = mock.Mock(side_effect=returns)
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(seconds=605),
            base_time + datetime.timedelta(seconds=905),
            ]
        timeutils.set_time_override_seq(time_seq)

        server = self.server_instance_fixture("test")
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        calls =[mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'PROCESSING', error_message=None, timeout=None),
                mock.call(job['id'], 'PROCESSING', error_message=None, timeout=None),
                mock.call(job['id'], 'DONE', error_message=None, timeout=None)]

        self.assertEqual('DONE', job['status'])
        worker.update_job.assert_has_calls(calls)

    def test_process_job_updates_timeout_and_status(self):
        job = self.job_fixture()
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        returns = [{'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'DONE',
                    'timeout': None, 'error_message': None}]
        worker = self.mock_worker()
        worker.update_job = mock.Mock(side_effect=returns)
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(minutes=60, seconds=5),
            base_time + datetime.timedelta(minutes=60, seconds=305),
            ]
        timeutils.set_time_override_seq(time_seq)

        server = self.server_instance_fixture("test")
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        calls =[mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'DONE', error_message=None, timeout=None)]

        self.assertEqual('DONE', job['status'])
        worker.update_job.assert_has_calls(calls)

    def test_process_job_times_out(self):
        job = self.job_fixture()
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'SAVING')]
        returns = [{'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'PROCESSING',
                    'timeout': None, 'error_message': None},
                   {'job_id': job['id'], 'status': 'TIMED_OUT',
                    'timeout': None, 'error_message': None}]
        worker = self.mock_worker()
        worker.update_job = mock.Mock(side_effect=returns)
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time + datetime.timedelta(minutes=60, seconds=5),
            base_time + datetime.timedelta(minutes=120, seconds=15),
            base_time + datetime.timedelta(minutes=180, seconds=305),
            base_time + datetime.timedelta(minutes=240, seconds=306)
            ]
        timeutils.set_time_override_seq(time_seq)

        server = self.server_instance_fixture("test")
        nova_client = self.mock_nova_client(server, images)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor.process_job(job)
        calls =[mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'PROCESSING', error_message=None, timeout=mock.ANY),
                mock.call(job['id'], 'TIMED_OUT', error_message=None, timeout=None)]

        self.assertEqual('TIMED_OUT', job['status'])
        worker.update_job.assert_has_calls(calls)

    def test_delete_schedule_positive_retention(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)
        nova_client.images.delete = mock.Mock()
        nova_client.rax_scheduled_images_python_novaclient_ext.get = mock.Mock(return_value=server)
        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor._delete_schedule = mock.Mock()
        processor._find_scheduled_images_for_server = mock.Mock(return_value=images)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])
        self.assertTrue(nova_client.images.delete.called)

    def test_delete_schedule_images_greater_than_retention(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)
        nova_client.images.delete = mock.Mock()
        nova_client.rax_scheduled_images_python_novaclient_ext.get = mock.Mock(return_value=server)
        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor._find_scheduled_images_for_server = mock.Mock(return_value=images)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])
        self.assertTrue(nova_client.images.delete.called)
        self.assertEqual(2, nova_client.images.delete.call_count)

    def test_delete_schedule_images_less_than_retention(self):
        job = self.job_fixture()
        worker = self.mock_worker()

        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)
        nova_client.images.delete = mock.Mock()
        nova_client.rax_scheduled_images_python_novaclient_ext.get = mock.Mock(return_value=server)
        nova_client_factory = self.mock_nova_client_factory(job, nova_client)

        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor._find_scheduled_images_for_server = mock.Mock(return_value=images)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])
        self.assertEqual(0, nova_client.images.delete.call_count)

    def test_delete_schedule_bad_retention(self):
        job = self.job_fixture()
        worker = self.mock_worker()
        qonosclient = worker.get_qonos_client()

        server = self.server_instance_fixture("test", retention='blah')
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)
        nova_client.images.list = mock.Mock(return_value=images)
        nova_client.images.delete = mock.Mock()
        nova_client.rax_scheduled_images_python_novaclient_ext.get = mock.Mock(return_value=server)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)
        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor._find_scheduled_images_for_server = mock.Mock(return_value=images)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])
        self.assertTrue(qonosclient.delete_schedule.called)

    def test_delete_schedule_zero_retention(self):
        job = self.job_fixture()
        worker = self.mock_worker()
        qonosclient = worker.get_qonos_client()

        server = self.server_instance_fixture("test", retention=0)
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)
        nova_client.images.list = mock.Mock(return_value=images)
        nova_client.images.delete = mock.Mock()
        nova_client.rax_scheduled_images_python_novaclient_ext.get = mock.Mock(return_value=server)

        nova_client_factory = self.mock_nova_client_factory(job, nova_client)
        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        processor._find_scheduled_images_for_server = mock.Mock(return_value=images)
        processor.process_job(job)
        self.assertEqual('DONE', job['status'])
        self.assertTrue(qonosclient.delete_schedule.called)

    def test_generate_image_name(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)
        worker = self.mock_worker()
        nova_client_factory = self.mock_nova_client_factory(job, nova_client)
        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        timeutils.set_time_override(datetime.datetime(2013, 3, 22, 22, 39, 27))
        timestamp = '1363991967'
        image_name = processor.generate_image_name("test")
        self.assertEqual(image_name, 'Daily-test-' + timestamp)

    def test_generate_image_name(self):
        job = self.job_fixture()
        server = self.server_instance_fixture("test", retention=2)
        images = [self.image_fixture('IMAGE_ID', 'QUEUED'),
                  self.image_fixture('IMAGE_ID', 'SAVING'),
                  self.image_fixture('IMAGE_ID', 'ACTIVE')]
        nova_client = self.mock_nova_client(server, images)
        worker = self.mock_worker()
        nova_client_factory = self.mock_nova_client_factory(job, nova_client)
        processor = snapshot.SnapshotProcessor()
        processor.init_processor(worker,
                                 nova_client_factory=nova_client_factory)
        timeutils.set_time_override(datetime.datetime(2013, 3, 22, 22, 39, 27))
        timestamp = '1363991967'
        fake_server_name = 'a' * 255
        expected_server_name = 'a' * (255 - len(timestamp) - len('Daily--'))
        image_name = processor.generate_image_name(fake_server_name)
        expected_image_name = 'Daily-' + expected_server_name + '-' + timestamp
        self.assertEqual(image_name, expected_image_name)

