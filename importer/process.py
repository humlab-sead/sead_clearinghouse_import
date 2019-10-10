# -*- coding: utf-8 -*-
import os
import time
import io
import logging

from . import model
from . import options
from . import utility
from . import parser

logger = logging.getLogger('Excel XML processor')

jj = os.path.join

class AppService:

    def __init__(self, opts):
        self.opts = opts
        assert os.environ.get('SEAD_CH_PASSWORD', None) != None, "fatal: environment variable SEAD_CH_PASSWORD not set!"
        db_opts = dict(
             database=opts.dbname,
             user=opts.dbuser,
             password=os.environ['SEAD_CH_PASSWORD'],
             host=opts.dbhost,
             port=opts.port
         )
        self.repository = model.SubmissionRepository(db_opts)

    def upload_xml(self, xml_filename, data_types=''):

        with io.open(xml_filename, mode="r", encoding="utf-8") as f:
            xml = f.read()

        submission_id = self.repository.add_xml(xml, data_types=data_types)

        return submission_id

    def process(self):

        option = self.opts

        try:

            basename = os.path.splitext(option.data_filename)[0]

            if option.skip is True:
                logger.info("Skipping: %s", basename)
                return

            timestamp = time.strftime("%Y%m%d-%H%M%S")

            log_filename = jj(option.output_folder, '{}_{}.log'.format(basename, timestamp))
            utility.setup_logger(logger, log_filename)

            logger.info('PROCESS OF %s STARTED', basename)

            if (option.submission_id or 0) == 0:

                if option.xml_filename is not None:
                    logger.info(' ---> UPLOADING EXISTING FILE {}'.format(option.xml_filename))
                else:
                    logger.info(' ---> PARSING EXCEL EXCEL')
                    option.xml_filename = parser.process_excel_to_xml(option, basename, timestamp)

                logger.info(' ---> UPLOAD STARTED!')
                option.submission_id = self.upload_xml(option.xml_filename, data_types=option.data_types)
                logger.info(' ---> UPLOAD DONE ID=%s', option.submission_id)

                logger.info(' ---> EXTRACT STARTED!')
                self.repository.extract_submission(option.submission_id)
                logger.info(' ---> EXTRACT DONE')

            else:
                self.repository.delete_submission(option.submission_id, clear_header=False, clear_exploded=False)
                logger.info(' ---> USING EXISTING DATA ID=%s', option.submission_id)

            logger.info(' ---> EXPLODE STARTED')
            self.repository.explode_submission(option.submission_id, p_dry_run=False, p_add_missing_columns=False)
            logger.info(' ---> EXPLODE DONE')

            self.repository.set_pending(option.submission_id)
            logger.info(' ---> PROCESS OF %s DONE', basename)

        except: # pylint: disable=bare-except
            logger.exception('ABORTED CRITICAL ERROR %s ', basename)

def main():

    opts = options.parse_args()

    logger.warning("Deploy target is %s on %s", opts.dbname, opts.dbhost)

    AppService(opts).process()

if __name__ == "__main__":
    main()
