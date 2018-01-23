"""Utility to provide submission and comment statistics in a subreddit."""
from __future__ import print_function
from collections import defaultdict
from datetime import datetime
from tempfile import mkstemp
import codecs
import gc
import logging
import requests
import sys
import os
import re
import time
from praw import Reddit
from prawcore.exceptions import RequestException
from six import iteritems, text_type as tt
from .helpers import AGENT, arg_parser, check_for_updates
###CUSTOM LOGGING FOR ALTCOIN APPLICATION
import log_service.logger_factory as lf
logger = lf.get_loggly_logger(__name__)

SECONDS_IN_A_DAY = 60 * 60 * 24
RE_WHITESPACE = re.compile(r'\s+')
TOP_VALUES = {'all', 'day', 'month', 'week', 'year'}
TOP_VALS_IN_SECONDS = {'day':SECONDS_IN_A_DAY, 'week':SECONDS_IN_A_DAY*7, 'month':SECONDS_IN_A_DAY*30, 'year':
                       SECONDS_IN_A_DAY*365}

#logger = logging.getLogger(__package__)

class MiniComment(object):
    """Provides a memory optimized version of a Comment."""

    __slots__ = ('author', 'created_utc', 'id', 'score', 'submission')

    def __init__(self, comment, submission):
        """Initialize an instance of MiniComment."""
        for attribute in self.__slots__:
            if attribute in {'author', 'submission'}:
                continue
            setattr(self, attribute, getattr(comment, attribute))
        self.author = str(comment.author) if comment.author else None
        self.submission = submission


class MiniSubmission(object):
    """Provides a memory optimized version of a Submission."""

    __slots__ = ('author', 'created_utc', 'distinguished', 'id',
                 'num_comments', 'permalink', 'score', 'title', 'url')

    def __init__(self, submission):
        """Initialize an instance of MiniSubmission."""
        for attribute in self.__slots__:
            if attribute == 'author':
                continue
            setattr(self, attribute, getattr(submission, attribute))
        self.author = str(submission.author) if submission.author else None


class SubredditStats(object):
    """Contain all the functionality of the subreddit_stats command."""

    post_footer = tt('>Generated with [BBoe](/u/bboe)\'s [Subreddit Stats]'
                     '(https://github.com/praw-dev/prawtools_fork) '
                     '([Donate](https://cash.me/$praw))')
    post_header = tt('---\n###{}\n')
    post_prefix = tt('Subreddit Stats:')

    @staticmethod
    def get_http(uri, parameters=None, max_retries=5, headers={'User-Agent': 'Mozilla/57.0 (Macintosh; '
        'Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}):
        """ Get data from API or download HTML, try each URI 5 times """
        with requests.Session() as s:
            a = requests.adapters.HTTPAdapter(max_retries)
            s.mount('https://', a)
            response = s.get(uri, params=parameters, headers=headers)
        return response

    @staticmethod
    def _permalink(item):
        if isinstance(item, MiniSubmission):
            return tt('/comments/{}').format(item.id)
        else:
            return tt('/comments/{}//{}?context=1').format(item.submission.id,
                                                           item.id)

    @staticmethod
    def _points(points):
        return '1 point' if points == 1 else '{} points'.format(points)

    @staticmethod
    def _rate(items, duration):
        return 86400. * items / duration if duration else items

    @staticmethod
    def _safe_title(submission):
        """Return titles with whitespace replaced by spaces and stripped."""
        return RE_WHITESPACE.sub(' ', submission.title).strip()

    @staticmethod
    def _save_report(title, body):
        descriptor, filename = mkstemp('.md', dir='.')
        os.close(descriptor)
        with codecs.open(filename, 'w', 'utf-8') as fp:
            fp.write('{}\n\n{}'.format(title, body))
        logger.info('Report saved to {}'.format(filename))

    @staticmethod
    def _user(user):
        return '_deleted_' if user is None else tt('/u/{}').format(user)

    def __init__(self, subreddit, site, distinguished, user_agent=None, reddit=None):
        """Initialize the SubredditStats instance with config options."""
        self.commenters = defaultdict(list)
        self.comments = []
        self.distinguished = distinguished
        self.min_date = 0
        self.max_date = time.time() - SECONDS_IN_A_DAY
        if isinstance(user_agent, dict):
            self.user_agent = user_agent
        else:
            self.user_agent = AGENT
        self.reddit = (reddit or
                       Reddit(site, check_for_updates=False, user_agent=self.user_agent))
        self.submissions = {}
        self.subreddit_statistics = {}
        self.submitters = defaultdict(list)
        self.subreddit_name = subreddit
        self.submit_subreddit = self.reddit.subreddit('subreddit_stats')
        self.subreddit = self.reddit.subreddit(subreddit)
        self.collection_interval = None

    def __reset__(self, subreddit):
        self.commenters = defaultdict(list)
        self.comments = []
        self.min_date = 0
        self.max_date = time.time() - SECONDS_IN_A_DAY
        self.submissions = {}
        self.subreddit_statistics = {}
        self.submitters = defaultdict(list)
        self.subreddit_name = subreddit
        self.subreddit = self.reddit.subreddit(subreddit)
        self.collection_interval = None
        self.collection_start_time = None
        gc.collect()

    def basic_stats(self, stats_only=True, tuple_format=True):
        """Return a markdown representation of simple statistics."""

        comment_score = sum(comment.score for comment in self.comments)
        if self.comments:
            comment_duration = (self.comments[-1].created_utc -
                                self.comments[0].created_utc)
            if self.collection_interval in TOP_VALUES and self.collection_interval is not "all":
                request_duration = TOP_VALS_IN_SECONDS[self.collection_interval]
                comment_rate = self._rate(len(self.comments), request_duration)
            else:
                comment_rate = self._rate(len(self.comments), comment_duration)

        else:
            comment_rate = 0

        if not isinstance(self.collection_start_time, int):
            error_msg = "Collection start time variable not initialized correctly"
            logging.error(error_msg)
            raise AttributeError(error_msg)

        submission_duration = self.max_date - self.min_date
        if self.collection_interval in TOP_VALUES and self.collection_interval is not "all":
            request_duration = TOP_VALS_IN_SECONDS[self.collection_interval]
            submission_rate = self._rate(len(self.submissions), request_duration)
        else:
            submission_rate = self._rate(len(self.submissions), submission_duration)
        submission_score = sum(sub.score for sub in self.submissions.values())
        subscribers, active_users = None, None
        try:
            subscribers, active_users = self.fetch_wiki_data(self.subreddit_name, header=self.user_agent)
        except Exception as e:
            error_msg = ("error in fetching wiki data, error message was %s") % str(e)
            logging.warn(error_msg)
        if tuple_format == True:
            stats_data = (self.subreddit_name, self.collection_interval, self.collection_start_time, int(self.min_date),
                          int(self.max_date), int(submission_duration), comment_score, int(comment_rate),
                          submission_score, int(submission_rate), len(self.comments), len(self.commenters),
                          len(self.submissions), len(self.submitters), subscribers, active_users)
        else:
            stats_data = {"subreddit":self.subreddit_name, "collection_interval":self.collection_interval,
                          "collection_start_time":self.collection_start_time ,"min_date":int(self.min_date),
                          "max_date":int(self.max_date),"comment_rate":int(comment_rate), "comment_score":comment_score,
                          "submission_interval":int(submission_duration), "submission_score": submission_score,
                          "submission_rate": int(submission_rate),"num_comments": len(self.comments),
                          "num_commenters": len(self.commenters), "num_submissions": len(self.submissions),
                          "num_submitters": len(self.submitters), "measurement_period":self.collection_interval,
                          "subscribers":subscribers, "active_users":active_users}
        if stats_only == True:
            return stats_data
        else:
            values = [('Total', len(self.submissions), len(self.comments)),
                      ('Rate (per day)', '{:.2f}'.format(submission_rate),
                       '{:.2f}'.format(comment_rate)),
                      ('Unique Redditors', len(self.submitters),
                       len(self.commenters)),
                      ('Combined Score', submission_score, comment_score)]

            retval = 'Period: {:.2f} days\n\n'.format(submission_duration / 86400.)
            retval += '||Submissions|Comments|\n:-:|--:|--:\n'
            for quad in values:
                retval += '__{}__|{}|{}\n'.format(*quad)
            return retval + '\n'

    def fetch_recent_submissions(self, max_duration):
        """Fetch recent submissions in subreddit with boundaries.

        Does not include posts within the last day as their scores may not be
        representative.

        :param max_duration: When set, specifies the number of days to include

        """
        if max_duration:
            self.min_date = self.max_date - SECONDS_IN_A_DAY * max_duration
        for submission in self.subreddit.new(limit=None):
            if submission.created_utc <= self.min_date:
                break
            if submission.created_utc > self.max_date:
                continue
            self.submissions[submission.id] = MiniSubmission(submission)

    def fetch_submissions(self, submissions_callback, *args):
        """Wrap the submissions_callback function."""
        logger.debug('Fetching submissions')

        submissions_callback(*args)

        logger.info('Found {} submissions'.format(len(self.submissions)))
        if not self.submissions:
            return

        self.min_date = min(x.created_utc for x in self.submissions.values())
        self.max_date = max(x.created_utc for x in self.submissions.values())

        self.process_submitters()
        self.process_commenters()

    def fetch_wiki_data(self, subreddit, header):
        wiki_url = "https://www.reddit.com/r/" + subreddit + "/about/.json"
        wiki_data = self.get_http(wiki_url, headers=header)
        if wiki_data.status_code == 200:
            wiki_data = wiki_data.json()
            return wiki_data["data"]["subscribers"], wiki_data["data"]["active_user_count"]

    def fetch_top_submissions(self, top):
        """Fetch top submissions by some top value.

        :param top: One of week, month, year, all
        :returns: True if any submissions were found.

        """
        for submission in self.subreddit.top(limit=None, time_filter=top):
            self.submissions[submission.id] = MiniSubmission(submission)

    def process_commenters(self):
        """Group comments by author."""
        for index, submission in enumerate(self.submissions.values()):
            if submission.num_comments == 0:
                continue
            real_submission = self.reddit.submission(id=submission.id)
            real_submission.comment_sort = 'top'

            for i in range(3):
                try:
                    real_submission.comments.replace_more(limit=0)
                    break
                except RequestException:
                    if i >= 2:
                        raise
                    logger.debug('Failed to fetch submission {}, retrying'
                                 .format(submission.id))

            self.comments.extend(MiniComment(comment, submission)
                                 for comment in real_submission.comments.list()
                                 if self.distinguished
                                 or comment.distinguished is None)

            if index % 50 == 49:
                logger.debug('Completed: {:4d}/{} submissions'
                             .format(index + 1, len(self.submissions)))

            # Clean up to reduce memory usage
            submission = None
            gc.collect()

        self.comments.sort(key=lambda x: x.created_utc)
        for comment in self.comments:
            if comment.author:
                self.commenters[comment.author].append(comment)

    def process_submitters(self):
        """Group submissions by author."""
        for submission in self.submissions.values():
            if submission.author and (self.distinguished or
                                      submission.distinguished is None):
                self.submitters[submission.author].append(submission)

    def process_submission_stats(self):
        """Return data from top submissions."""
        num = len(self.submissions)
        if num <= 0:
            return []
        st = self.collection_start_time
        submissions =  [(s_id, st, x.title, self.subreddit_name, x.score, x.num_comments, int(x.created_utc), x.author)
            for s_id, x in self.submissions.iteritems() if self.distinguished or x.distinguished is None]
        return submissions

    def publish_results(self, view, submitters, commenters, publish_externally=False):
        """Submit the results to the subreddit. Has no return value (None)."""
        def timef(timestamp, date_only=False):
            """Return a suitable string representaation of the timestamp."""
            dtime = datetime.fromtimestamp(timestamp)
            if date_only:
                retval = dtime.strftime('%Y-%m-%d')
            else:
                retval = dtime.strftime('%Y-%m-%d %H:%M PDT')
            return retval

        if publish_externally==False:
            basic = self.basic_stats(stats_only=True)
            submissions = self.process_submission_stats()

            natural_language_data = {"submissions":submissions}
            subreddit_statistics = {"numerical_stats":basic, "subreddit_name":self.subreddit_name,
                                    "natural_language_data":natural_language_data, "time_recorded":time.time()}
            self.subreddit_statistics = subreddit_statistics
            return subreddit_statistics


        def publish_to_subbreddit(submitters):
            basic = self.basic_stats()
            top_commenters = self.top_commenters(commenters)
            top_comments = self.top_comments()
            top_submissions = self.top_submissions()
            # Decrease number of top submitters if body is too large.
            body = None
            while body is None or len(body) > 40000 and submitters > 0:
                body = (basic + self.top_submitters(submitters) + top_commenters
                        + top_submissions + top_comments + self.post_footer)
                submitters -= 1

            title = '{} {} {}posts from {} to {}'.format(
                self.post_prefix, str(self.subreddit),
                'top ' if view in TOP_VALUES else '', timef(self.min_date, True),
                timef(self.max_date))

            try:  # Attempt to make the submission
                return self.submit_subreddit.submit(title, selftext=body)
            except Exception:
                logger.exception('Failed to submit to {}'
                                 .format(self.submit_subreddit))
                self._save_report(title, body)
        if publish_externally == True:
            publish_to_subbreddit(submitters)

    def run(self, view, submitters, commenters):
        """Run stats and return the created Submission."""
        logger.info('Analyzing subreddit: {}'.format(self.subreddit))
        self.collection_interval = view
        self.collection_start_time = int(time.time())
        if view in TOP_VALUES:
            callback = self.fetch_top_submissions
        else:
            callback = self.fetch_recent_submissions
            view = int(view)

        self.fetch_submissions(callback, view)

        if not self.submissions:
            logger.warning('No submissions were found.')
            return

        return self.publish_results(view, submitters, commenters)

    def top_commenters(self, num, give_list=False):
        """Return a markdown representation of the top commenters."""
        num = min(num, len(self.commenters))
        if num <= 0:
            if give_list == True:
                return []
            else:
                return ''

        top_commenters = sorted(
            iteritems(self.commenters),
            key=lambda x: (-sum(y.score for y in x[1]),
                           -len(x[1]), str(x[0])))[:num]

        if give_list == True:
            return top_commenters

        retval = self.post_header.format('Top Commenters')
        for author, comments in top_commenters:
            retval += '0. {} ({}, {} comment{})\n'.format(
                self._user(author),
                self._points(sum(x.score for x in comments)),
                len(comments), 's' if len(comments) != 1 else '')
        return '{}\n'.format(retval)

    def top_submitters(self, num, give_list=False):
        """Return a markdown representation of the top submitters."""
        num = min(num, len(self.submitters))
        if num <= 0:
            if give_list == True:
                return []
            else:
                return ''

        top_submitters = sorted(
            iteritems(self.submitters),
            key=lambda x: (-sum(y.score for y in x[1]),
                           -len(x[1]), str(x[0])))[:num]
        if give_list == True:
            return top_submitters

        retval = self.post_header.format('Top Submitters\' Top Submissions')
        for (author, submissions) in top_submitters:
            retval += '0. {}, {} submission{}: {}\n'.format(
                self._points(sum(x.score for x in submissions)),
                len(submissions),
                's' if len(submissions) != 1 else '', self._user(author))
            for sub in sorted(
                    submissions, key=lambda x: (-x.score, x.title))[:10]:
                title = self._safe_title(sub)
                if sub.permalink in sub.url:
                    retval += tt('  0. {}').format(title)
                else:
                    retval += tt('  0. [{}]({})').format(title, sub.url)
                retval += ' ({}, [{} comment{}]({}))\n'.format(
                    self._points(sub.score), sub.num_comments,
                    's' if sub.num_comments != 1 else '',
                    self._permalink(sub))
            retval += '\n'
        return retval

    def top_submissions(self, give_list=False):
        """Return a markdown representation of the top submissions."""
        num = min(10, len(self.submissions))
        if num <= 0:
            return ''

        top_submissions = sorted(
            [x for x in self.submissions.values() if self.distinguished or
             x.distinguished is None],
            key=lambda x: (-x.score, -x.num_comments, x.title))[:num]

        if not top_submissions:
            if give_list == True:
                return top_submissions
            else:
                return ''

        if give_list == True:
            return top_submissions

        retval = self.post_header.format('Top Submissions')
        for sub in top_submissions:
            title = self._safe_title(sub)
            if sub.permalink in sub.url:
                retval += tt('0. {}').format(title)
            else:
                retval += tt('0. [{}]({})').format(title, sub.url)

            retval += ' by {} ({}, [{} comment{}]({}))\n'.format(
                self._user(sub.author), self._points(sub.score),
                sub.num_comments, 's' if sub.num_comments != 1 else '',
                self._permalink(sub))
        return tt('{}\n').format(retval)

    def top_comments(self, give_list=False):
        """Return a markdown representation of the top comments."""
        num = min(10, len(self.comments))
        if num <= 0:
            if give_list == True:
                return []
            else:
                return ''

        top_comments = sorted(
            self.comments, key=lambda x: (-x.score, str(x.author)))[:num]

        if give_list == True:
            return top_comments

        retval = self.post_header.format('Top Comments')
        for comment in top_comments:
            title = self._safe_title(comment.submission)
            retval += tt('0. {}: {}\'s [comment]({}) in {}\n').format(
                self._points(comment.score), self._user(comment.author),
                self._permalink(comment), title)
        return tt('{}\n').format(retval)


def main():
    """Provide the entry point to the subreddit_stats command."""
    parser = arg_parser(usage='usage: %prog [options] SUBREDDIT VIEW')
    parser.add_option('-c', '--commenters', type='int', default=10,
                      help='Number of top commenters to display '
                      '[default %default]')
    parser.add_option('-d', '--distinguished', action='store_true',
                      help=('Include distinguished subissions and '
                            'comments (default: False). Note that regular '
                            'comments of distinguished submissions will still '
                            'be included.'))
    parser.add_option('-s', '--submitters', type='int', default=10,
                      help='Number of top submitters to display '
                      '[default %default]')

    options, args = parser.parse_args()

    if options.verbose == 1:
        logger.setLevel(logging.INFO)
    elif options.verbose > 1:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.NOTSET)
    logger.addHandler(logging.StreamHandler())

    if len(args) != 2:
        parser.error('SUBREDDIT and VIEW must be provided')
    subreddit, view = args
    check_for_updates(options)
    srs = SubredditStats(subreddit, options.site, options.distinguished)
    result = srs.run(view, options.submitters, options.commenters)
    if result:
        print(result.permalink)
    return 0
