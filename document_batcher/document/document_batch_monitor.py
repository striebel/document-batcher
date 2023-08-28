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
        logger                                  : Logger               = None, # required
        input_file                              : InputFile            = None, # required
        time_budget_in_hours                    : int                  = 24    # optional
    ) -> DocumentBatchMonitor:
        assert isinstance(input_file, InputFile)
        assert isinstance(logger, Logger)
        assert isinstance(time_budget_in_hours, int)
        assert 0 < time_budget_in_hours
        
        self._input_file           = input_file
        self._logger               = logger
        self._time_budget_in_hours = time_budget_in_hours
        
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
        
        
        
        #######################################################################
        #### update accumulators
        
        self._n_docs  += batch_n_docs
        self._n_sents += batch_n_sents
        self._n_words += batch_n_words
        self._n_chars += batch_n_chars
        self._n_secs  += batch_n_secs
        
        

        #######################################################################
        #### calcuate progress statistics

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
        #### log progress statistics
        
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
    



