import logging
from tests.server_testcase import RequestTestCase


log = logging.getLogger(__name__)


class TestOsdManagement(RequestTestCase):
    def setUp(self):
        super(TestOsdManagement, self).setUp()
        self.ceph_ctl.configure(3)
        self.calamari_ctl.configure()

    def test_update(self):
        """
        That valid updates succeed and take effect
        """
        updates = {
            'in': False,
            'up': False,
            'reweight': 0.5
        }

        osd_id = 0
        fsid = self._wait_for_cluster()

        for k, v in updates.items():
            log.debug("Updating %s=%s" % (k, v))
            osd_url = "cluster/%s/osd/%s" % (fsid, osd_id)

            # Check that the modification really is a modification and
            # not the status quo
            osd = self.api.get(osd_url).json()
            self.assertNotEqual(osd[k], v)

            # Apply the modification
            response = self.api.patch(osd_url, {k: v})
            self._wait_for_completion(fsid, response)

            # After completion the update should be visible
            # NB this is slightly racy on a real cluster because when we mark something
            # down it will at some point get marked up again, hopefully not so quickly
            # that we fail this check.  But if this is mysteriously failing, yeah.  That.

            osd = self.api.get(osd_url).json()
            self.assertEqual(osd[k], v)

    def test_no_op_updates(self):
        """
        That no-op updates get a 304 response
        """
        no_op_updates = {
            'in': True,
            'up': True,
            'reweight': 1.0
        }

        osd_id = 0
        fsid = self._wait_for_cluster()

        for k, v in no_op_updates.items():
            osd_url = "cluster/%s/osd/%s" % (fsid, osd_id)
            osd = self.api.get(osd_url).json()
            self.assertEqual(osd[k], v)
            response = self.api.patch(osd_url, {k: v})
            self.assertEqual(response.status_code, 304)
            osd = self.api.get(osd_url).json()
            self.assertEqual(osd[k], v)