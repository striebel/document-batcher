from __future__ import annotations
from logging import Logger
from datetime import datetime
from textwrap import dedent
from time import sleep
import math


from ..io.input_file import InputFile


class DocumentBatchMonitor():

    def __init__(
        self                                    : DocumentBatchMonitor,
        input_file                              : InputFile,
        logger                                  : Logger,
        time_budget_in_hours                    : int,
        processing_slowdown_restart_window_size : int,
        processing_slowdown_restart_factor      : float,
        ########################################################################
        #### parameters for testing
        processing_slowdown_restart_test_run_test                 : bool,
        processing_slowdown_restart_test_num_doc_batches_to_delay : int | None
    ) -> DocumentBatchMonitor:

        assert isinstance(input_file, InputFile)
        assert isinstance(logger, Logger)
        assert isinstance(time_budget_in_hours, int)
        assert 0 < time_budget_in_hours
        assert isinstance(processing_slowdown_restart_window_size, int)
        assert 0 < processing_slowdown_restart_window_size
        assert isinstance(processing_slowdown_restart_factor, float)
        assert 1.0 < processing_slowdown_restart_factor
        if processing_slowdown_restart_test_run_test is True:
            assert isinstance(
                processing_slowdown_restart_test_num_doc_batches_to_delay, int
            )
            assert 0 <= processing_slowdown_restart_test_num_doc_batches_to_delay
        else:
            assert processing_slowdown_restart_test_run_test is False
            assert processing_slowdown_restart_test_num_doc_batches_to_delay is None
        
        ########################################################################
        #### parameters for core functionality
        self._input_file         = input_file
        self._logger               = logger
        self._time_budget_in_hours = time_budget_in_hours
        self._processing_slowdown_restart_window_size = \
            processing_slowdown_restart_window_size
        self._processing_slowdown_restart_factor = \
            processing_slowdown_restart_factor
        
        ########################################################################
        #### parameters for testing processing slowdown restart functionality
        self._processing_slowdown_restart_test_run_test = \
            processing_slowdown_restart_test_run_test
        self._processing_slowdown_restart_test_num_doc_batches_to_delay = \
            processing_slowdown_restart_test_num_doc_batches_to_delay
        
        
        ########################################################################
        #### class instance private variables for processing slowdown restart
        #### functionality
        self._batch_sentence_rates_in_window = []
        self._doc_batch_index_that_filled_window = -1
        self._average_batch_sentence_rate_in_window_record_high = 0.0

        ########################################################################
        #### running totals for num docs, sents, words, and chars processed
        #### so far
        self._n_docs  = 0
        self._n_sents = 0
        self._n_words = 0
        self._n_chars = 0
        self._n_secs  = 0
        self._index   = 0


    def get_current_doc_batch_index(
        self : DocumentBatchMonitor
    ) -> int:

        assert isinstance(self._index, int)
        assert 0 <= self._index

        return self._index


    def processing_slowdown_restart_test_run_test_is_true(
        self : DocumentBatchMonitor
    ) -> bool:

        assert isinstance(self._processing_slowdown_restart_test_run_test, bool)

        return self._processing_slowdown_restart_test_run_test


    def processing_slowdown_restart_test_ready(
        self                    : DocumentBatchMonitor,
        current_doc_batch_index : int
    ) -> None:
        
        assert self._processing_slowdown_restart_test_run_test_is_true()

        if self._doc_batch_index_that_filled_window < 0:

            return False

        assert (
            self._doc_batch_index_that_filled_window
            <= current_doc_batch_index
        )

        num_doc_batches_delayed_so_far = \
            current_doc_batch_index - 1 \
            - self._doc_batch_index_that_filled_window
        
        assert 0 <= num_doc_batches_delayed_so_far

        self._logger.debug(
            dedent(
                '''\
                timing_handler:
                DocumentBatchMonitor.processing_slowdown_restart_test_ready:
                num_doc_batches_delayed_so_far: {0}'''
            ).replace('\n', ' ').format(
                num_doc_batches_delayed_so_far
            )
        )
        
        if (
            num_doc_batches_delayed_so_far
            < self._processing_slowdown_restart_test_num_doc_batches_to_delay
        ):
            
            return False

        else:

            assert (
                self._processing_slowdown_restart_test_num_doc_batches_to_delay
                <= num_doc_batches_delayed_so_far
            )

            assert (
                len(self._batch_sentence_rates_in_window)
                == self._processing_slowdown_restart_window_size
            )

            assert 0.0 < self._average_batch_sentence_rate_in_window_record_high

            return True
    
    
    def processing_slowdown_restart_test_instigate_slowdown(
        self                               : DocumentBatchMonitor,
        current_doc_batch_index            : int,
        num_sentences_in_current_doc_batch : int
    ) -> None:

        assert isinstance(self, DocumentBatchMonitor)
        assert isinstance(current_doc_batch_index, int)
        assert 0 <= current_doc_batch_index
        assert isinstance(num_sentences_in_current_doc_batch, int)
        assert 0 < num_sentences_in_current_doc_batch

        assert self._processing_slowdown_restart_test_run_test_is_true()

        assert self._processing_slowdown_restart_test_ready(
            current_doc_batch_index = current_doc_batch_index
        )

        assert (
            len(self._batch_sentence_rates_in_window)
            == self._processing_slowdown_restart_window_size
        )

        assert 0.0 < self._average_batch_sentence_rate_in_window_record_high

        upper_bound_on_average_batch_sentence_rate_to_achieve_restart = \
            self._average_batch_sentence_rate_in_window_record_high \
            / self._processing_slowdown_restart_factor

        lower_bound_on_seconds_of_sleep_per_batch_to_achieve_restart = \
            num_sentences_in_current_doc_batch \
            / upper_bound_on_average_batch_sentence_rate_to_achieve_restart
        
        self._logger.debug(
            dedent(
                '''\
                timing_handler:
                DocumentBatchMonitor.processing_slowdown_restart_test_instigate_slowdown:
                will now sleep for {0:.4} seconds'''
            ).replace('\n', ' ').format(
                lower_bound_on_seconds_of_sleep_per_batch_to_achieve_restart
            )
        )
        
        sleep(lower_bound_on_seconds_of_sleep_per_batch_to_achieve_restart)


    def check_for_processing_slowdown(
        self                    : DocumentBatchMonitor,
        global_start_time       : datetime,
        current_doc_batch_index : int
    ) -> None:
        
        assert isinstance(self, DocumentBatchMonitor)
        assert isinstance(global_start_time, datetime)
        assert isinstance(current_doc_batch_index, int)
        assert 0 <= current_doc_batch_index
        
        if (
            len(self._batch_sentence_rates_in_window)
            < self._processing_slowdown_restart_window_size
        ):
            return
        
        assert (
            len(self._batch_sentence_rates_in_window)
            == self._processing_slowdown_restart_window_size
        )

        average_batch_sentence_rate_in_window = \
            sum(self._batch_sentence_rates_in_window) \
            / self._processing_slowdown_restart_window_size
        
        if (
            self._average_batch_sentence_rate_in_window_record_high
            < average_batch_sentence_rate_in_window
        ):
            self._average_batch_sentence_rate_in_window_record_high = \
                average_batch_sentence_rate_in_window
            return
        
        if (
            average_batch_sentence_rate_in_window
            < (
                self._average_batch_sentence_rate_in_window_record_high
                / self._processing_slowdown_restart_factor
            )
        ):
            self._logger.debug(
                dedent(
                    '''\
                    timing_handler:
                    DocumentBatchMonitor.check_for_processing_slowdown:
                    slowdown has occurred, restarting'''
                ).replace('\n', ' ')
            )

            newly_elapsed_seconds = int(
                (datetime.now() - global_start_time).total_seconds()
            )

            if self.completion_report_file is not None:

                assert isinstance(self.completion_report_file, str)

                if os.path.isfile(self.completion_report_file):

                    completion_report_file_obj = \
                        open(self.completion_report_file, 'r')

                    completion_report_str = \
                        completion_report_file_obj.read().strip()

                    assert 0 < len(completion_report_str)

                    completion_report_dict = json.loads(completion_report_str)

                    assert (
                        'processing_slowdown_restart_num_restarts'
                        in completion_report_dict
                    )

                    assert 0 < completion_report_dict[
                        'processing_slowdown_restart_num_restarts'
                    ]

                    completion_report_file_obj.close()

                else:

                    completion_report_dict = {
                        'processing_slowdown_restart_num_restarts': 0
                    }

                completion_report_dict[
                    'processing_slowdown_restart_num_restarts'
                ] += 1

                completion_report_file_obj = \
                    open(self.completion_report_file, 'w')

                completion_report_file_obj.write(
                    json.dumps(completion_report_dict)
                )

                completion_report_file_obj.close()

            self.error_handler.recover(
                doc_batch_start       = current_doc_batch_index,
                newly_elapsed_seconds = newly_elapsed_seconds
            )


    def post_batch_update(
        self          : DocumentBatchMonitor,
        batch_n_docs  : int,
        batch_n_sents : int,
        batch_n_words : int,
        batch_n_chars : int,
        batch_n_secs  : int
    ) -> None:
        
        METHOD_NAME = 'document.document_batch_monitor: ' \
            + 'DocumentBatchMonitor.post_batch_update:'
        

        ########################################################################
        #### validate method invocation preconditions
        
        assert isinstance(self._input_file, InputFile)
        assert isinstance(self._logger, Logger)
        assert isinstance(self._time_budget_in_hours, int)
        assert 0 < self._time_budget_in_hours
        
        assert isinstance(self._n_docs, int)
        assert 0 <= self._n_docs
        assert isinstance(self._n_sents, int)
        assert 0 <= self._n_sents
        assert isinstance(self._n_words, int)
        assert 0 <= self._n_words
        assert isinstance(self._n_chars, int)
        assert 0 <= self._n_chars
        assert isinstance(self._n_secs, int)
        assert 0 <= self._n_secs
        assert isinstance(self._index, int)
        assert 0 <= self._index

        assert isinstance(self._processing_slowdown_restart_window_size, int)
        assert 0 < self._processing_slowdown_restart_window_size
        assert isinstance(self._processing_slowdown_restart_factor, float)
        assert 1.0 < self._processing_slowdown_restart_factor
        assert isinstance(self._batch_sentence_rates_in_window, list)
        assert 0 <= len(self._batch_sentence_rates_in_window)
        assert (
            len(self._batch_sentence_rates_in_window)
            <= self._processing_slowdown_restart_window_size
        )
        for batch_sentence_rate in self._batch_sentence_rates_in_window:
            assert isinstance(batch_sentence_rate, float)
            assert 0.0 < batch_sentence_rate
        assert isinstance(self._average_batch_sentence_rate_in_window_record_high, float)
        assert 0.0 <= self._average_batch_sentence_rate_in_window_record_high
        
        
        ########################################################################
        #### validate method invocation arguments

        assert isinstance(batch_n_docs, int)
        assert 0 < batch_n_docs
        assert isinstance(batch_n_sents, int)
        assert 0 < batch_n_sents
        assert isinstance(batch_n_words, int)
        assert 0 < batch_n_words
        assert isinstance(batch_n_chars, int)
        assert 0 < batch_n_chars
        assert isinstance(batch_n_secs, int)
        assert 0 < batch_n_secs
        
        
        
        
        self._n_docs  += batch_n_docs
        self._n_sents += batch_n_sents
        self._n_words += batch_n_words
        self._n_chars += batch_n_chars
        self._n_secs  += batch_n_secs
        
        
        total_hours   = math.floor(self._n_secs / 60 / 60)
        total_minutes = math.floor(self._n_secs / 60) - (total_hours * 60)
        total_seconds = self._n_secs - (total_hours * 60 * 60) - (total_minutes * 60)
    
        batch_minutes = math.floor(batch_n_secs / 60)
        batch_seconds = batch_n_secs - (batch_minutes * 60)
    
        total_doc_rate  = self._n_docs  / self._n_secs
        total_sent_rate = self._n_sents / self._n_secs
        total_word_rate = self._n_words / self._n_secs
        total_char_rate = self._n_chars / self._n_secs
        
        batch_doc_rate  = batch_n_docs  / batch_n_secs
        batch_sent_rate = batch_n_sents / batch_n_secs
        batch_word_rate = batch_n_words / batch_n_secs
        batch_char_rate = batch_n_chars / batch_n_secs
    
        

        ########################################################################
        #### processing slowdown restart: maintain necessary statistics

        if (
            len(self._batch_sentence_rates_in_window)
            < self._processing_slowdown_restart_window_size - 1
        ):
            assert -1 == self._doc_batch_index_that_filled_window

            self._batch_sentence_rates_in_window.append(batch_sent_rate)

        elif (
            len(self._batch_sentence_rates_in_window)
            == self._processing_slowdown_restart_window_size - 1
        ):
            assert -1 == self._doc_batch_index_that_filled_window

            self._doc_batch_index_that_filled_window = self._index

            self._batch_sentence_rates_in_window.append(batch_sentence_rate)

        elif (
            len(self._batch_sentence_rates_in_window)
            == self._processing_slowdown_restart_window_size
        ):
            assert 0 <= self._doc_batch_index_that_filled_window

            self._batch_sentence_rates_in_window.append(batch_sentence_rate)

            self._batch_sentence_rates_in_window.pop(0)

            assert (
                len(self._batch_sentence_rates_in_window)
                == self._processing_slowdown_restart_window_size
            )

        else:

            assert (
                self._processing_slowdown_restart_window_size
                < len(self._batch_sentence_rates_in_window)
            )

            assert False, \
                dedent(
                    '''\
                    {0}
                    processing_slowdown_restart_window was overfilled'''
                ).replace('\n', ' ').format(
                    METHOD_NAME
                )
        
        
        time_budget_in_seconds = self._time_budget_in_hours * 60 * 60
        
        fraction_of_time_budget_consumed = self._n_secs / time_budget_in_seconds
        
        
        assert isinstance(self._input_file.get_file_len_in_chars(), int)
        assert 0 < self._input_file.get_file_len_in_chars()
        
        num_chars_remaining = self._input_file.get_file_len_in_chars() - self._n_chars
        
        estimated_time_remaining_in_seconds = num_chars_remaining / total_char_rate
        
        estimated_remaining_hours   = math.floor(estimated_time_remaining_in_seconds / 60 / 60)
        estimated_remaining_minutes = math.floor(estimated_time_remaining_in_seconds / 60) - (estimated_remaining_hours * 60)
        estimated_remaining_seconds = estimated_time_remaining_in_seconds - (estimated_remaining_hours * 60 * 60) - (estimated_remaining_minutes * 60)
        
        estimated_total_hours   = total_hours   + estimated_remaining_hours
        estimated_total_minutes = total_minutes + estimated_remaining_minutes
        estimated_total_seconds = total_seconds + estimated_remaining_seconds
        
        assert isinstance(self._time_budget_in_hours, int)
        assert 0 < self._time_budget_in_hours
        assert 'time_budget_in_seconds' in locals()
         
        estimated_total_time_in_seconds = self._n_secs + estimated_time_remaining_in_seconds
    
        estimated_fraction_of_time_budget_needed = estimated_total_time_in_seconds / time_budget_in_seconds
    
    
        
        ########################################################################
        #### log doc processing progress information
        
        self._logger.info(
            dedent(
                '''\
                {0}
                doc processing progress information follows for doc batch index {1}'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                self._index
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                the last doc batch took {1} minutes {2} seconds'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                batch_minutes,
                batch_seconds
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                in the last doc batch,
                {1} docs containing
                {2} sents, {3} words, and {4} chars
                were processed'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                batch_n_docs,
                batch_n_sents,
                batch_n_words,
                batch_n_chars
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                in the last doc batch,
                the doc  parsing rate was {1:.4f} docs/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                batch_doc_rate
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                in the last doc batch,
                the sent parsing rate was {1:.4f} sents/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                batch_sent_rate
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                in the last doc batch,
                the word parsing rate was {1:.4f} words/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                batch_word_rate
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                in the last doc batch,
                the char parsing rate was {1:.4f} chars/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                batch_char_rate
            )
        )
    
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began, {1} hours {2} minutes {3} seconds
                have elapsed'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                total_hours,
                total_minutes,
                total_seconds
            )
        )

        assert 'fraction_of_time_budget_consumed' in locals()
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                {1:.4} fraction of the time budget of {2} hours
                has been used'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                fraction_of_time_budget_consumed,
                self._time_budget_in_hours
            )
        )
    
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                {1} (of {2}) docs  have been processed: {3}'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                self._n_docs,
                self._input_file.get_file_len_in_docs(),
                self._n_docs / self._input_file.get_file_len_in_docs()
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                {1} (of {2}) sents have been processed: {3}'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                self._n_sents,
                self._input_file.get_file_len_in_sents(),
                self._n_sents / self._input_file.get_file_len_in_sents()
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                {1} (of {2}) words have been processed: {3}'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                self._n_words,
                self._input_file.get_file_len_in_words(),
                self._n_words / self._input_file.get_file_len_in_words()
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                {1} (of {2}) chars have been processed: {3}'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                self._n_chars,
                self._input_file.get_file_len_in_chars(),
                self._n_chars / self._input_file.get_file_len_in_chars()
            )
        )

        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                the average doc  parsing rate is {1:.4f} docs/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                total_doc_rate
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                the average sent parsing rate is {1:.4f} sents/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                total_sent_rate
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                the average word parsing rate is {1:.4f} words/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                total_word_rate
            )
        )
        self._logger.info(
            dedent(
                '''\
                {0}
                since doc processing began,
                the average char parsing rate is {1:.4f} char/s'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                total_char_rate
            )
        )
        
        assert 'estimated_remaining_hours'   in locals()
        assert 'estimated_remaining_minutes' in locals()
        assert 'estimated_remaining_seconds' in locals()
    
        self._logger.info(
            dedent(
                '''\
                {0}
                at the current average char parsing rate,
                {1} hours {2} minutes {3} seconds
                remain until all the docs have been processed'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                estimated_remaining_hours,
                estimated_remaining_minutes,
                math.ceil(estimated_remaining_seconds)
            )
        )
        
        assert 'estimated_total_hours'   in locals()
        assert 'estimated_total_minutes' in locals()
        assert 'estimated_total_seconds' in locals()
    
        self._logger.info(
            dedent(
                '''\
                {0}
                at the current average char parsing rate,
                {1} hours {2} minutes {3} seconds
                will be the total time used to process
                all the docs'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                estimated_total_hours,
                estimated_total_minutes,
                math.ceil(estimated_total_seconds)
            )
        )
    
        assert 'estimated_fraction_of_time_budget_needed' in locals()
            
        self._logger.info(
            dedent(
                '''\
                {0}
                at the current average char parsing rate,
                {1:.4} will be the total fraction
                of the time budget of {2} hours
                needed to process all the docs'''
            ).replace('\n', ' ').format(
                METHOD_NAME,
                estimated_fraction_of_time_budget_needed,
                self._time_budget_in_hours
            )
        )

        self._index += 1
    



