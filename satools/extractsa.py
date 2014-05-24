#
# Copyright 2014 Red Hat, Inc.
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os, tempfile, time
from ctypes import Structure

from satools.sysstat import ContentAction, TWO_DAYS_SECONDS, FileActivitySummary


class ExtractAction(ContentAction):
    """
    Extract records to an saYYYYMMDD file. If disjoint datasets are
    encountered, write them to separate new data files.
    """
    def __init__(self, tgtdname):
        self.tgtname = ""
        self.ofd = None
        self.ofname = ""
        self.tgtdname = tgtdname
        self.record_count = 0
        self.file_magic = None
        self.file_header = None
        self.file_activities = None
        self.extracted = False

    def _setup(self):
        self.ofd, self.ofname = tempfile.mkstemp(dir=self.tgtdname)
        st = time.gmtime(self.file_header.sa_ust_time)
        self.tgtname = "sa%04d%02d%02d" % (st.tm_year, st.tm_mon, st.tm_mday)
        if self.file_magic is not None:
            os.write(self.ofd, self.file_magic)
        os.write(self.ofd, self.file_header)
        if self.file_activities is not None:
            for a_fa in self.file_activities.fa:
                os.write(self.ofd, a_fa)

    def start(self, file_magic=None, file_header=None, file_activities=None):
        assert file_magic is None or isinstance(file_magic, Structure)
        assert isinstance(file_header, Structure)
        if file_activities is not None:
            isinstance(file_activities, FileActivitySummary)
            for a_fa in file_activities.fa:
                assert isinstance(a_fa, Structure)

        self.file_magic = file_magic
        self.file_header = file_header
        self.file_activities = file_activities
        self._setup()

    def handle_record(self, rh, record_payload=None):
        os.write(self.ofd, rh)
        self.record_count += 1
        if record_payload is None:
            return
        if isinstance(record_payload, list):
            for item in record_payload:
                os.write(self.ofd, item)
        else:
            os.write(self.ofd, record_payload)

    def handle_invalid(self, rh, prev_rh):
        assert rh is not None

        if prev_rh is None:
            return True

        if (rh.ust_time - prev_rh.ust_time) < TWO_DAYS_SECONDS:
            return True

        # Finalize what we have captured so far
        self.end()

        # Now we need to update the file header timestamp to make
        # the current record, and continue from there as if
        # nothing happened.
        self.file_header.sa_ust_time = rh.ust_time
        st = time.gmtime(self.file_header.sa_ust_time)
        self.file_header.sa_day = st.tm_mday
        # A time.time_struct's tm_mon field is 1 - 12, but the file header
        # sa_month is field is from a C "struct tm" which is 0 - 11.
        self.file_header.sa_month = st.tm_mon - 1
        # A time.time_struct's tm_year field is the full year value, but
        # the file header sa_year is field is # of years since 1900.
        self.file_header.sa_year = st.tm_year - 1900

        # Prepare for a new file.
        self.record_count = 0
        self.tgtname = ""
        self.ofd = None
        self.ofname = ""
        self._setup()

        return False

    def end(self):
        # Perform temp file cleanup, and rename into place
        if self.ofd is None:
            return
        os.close(self.ofd)
        self.ofd = None
        if self.record_count == 0:
            os.unlink(self.ofname)
            return
        full_tgtname = os.path.join(self.tgtdname, self.tgtname)
        if os.path.exists(full_tgtname):
            os.unlink(self.ofname)
        else:
            os.rename(self.ofname, full_tgtname)
            print "Extracted %d records to %s" % (self.record_count, full_tgtname)
            self.extracted = True
