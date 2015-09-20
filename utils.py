from itertools import zip_longest
import logging
from requests.exceptions import RequestException
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests.exceptions import SSLError
from retrying import retry
import tokenize
import token


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def get_nested_items(obj, *names):
    """Return obj[name][name2] ... [nameN] for any list of names."""
    for name in names:
        obj = obj[name]
    return obj


def configure_logging(logger_name, args=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    if args is None:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    elif args.fork:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(processName)s - %(levelname)s - %(message)s')
    else:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')

    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=120000)
def write_points(logger, client, json_points, line_number):
    # TODO - I originally wrote this to reduce code duplication - however, we need a better way to handle all the parameters
    # TODO - We need a better way to handle retry behaviour, without needing to re-raise exceptions
    try:
        if json_points:
            client.write_points(json_points)
            logger.info("Wrote in {} points to InfluxDB. Processed up to line {:<-8}".format(len(json_points), line_number))
        else:
            logger.info("Points are empty on line {}, skipping. Please check your data set.".format( line_number))
    except RequestException as e:
        logger.error("Unable to connect to InfluxDB at {} - {}".format(client._host, e))
        logger.info("Retrying...")
        raise Exception
    except InfluxDBClientError as e:
        logger.error('Unable to write to InfluxDB - {}'.format(e))
        logger.info("Retrying...")
        raise Exception
    except SSLError as e:
        logger.error('SSL error - {}'.format(e))
        logger.info("Retrying...")
        raise Exception

# borrowed from http://stackoverflow.com/questions/4033633/handling-lazy-json-in-python-expecting-property-name
def fixLazyJsonWithComments (in_text):
  """ Same as fixLazyJson but removing comments as well
  """
  result = []
  tokengen = tokenize.generate_tokens(StringIO(in_text).readline)

  sline_comment = False
  mline_comment = False
  last_token = ''

  for tokid, tokval, _, _, _ in tokengen:

    # ignore single line and multi line comments
    if sline_comment:
      if (tokid == token.NEWLINE) or (tokid == tokenize.NL):
        sline_comment = False
      continue

    # ignore multi line comments
    if mline_comment:
      if (last_token == '*') and (tokval == '/'):
        mline_comment = False
      last_token = tokval
      continue

    # fix unquoted strings
    if (tokid == token.NAME):
      if tokval not in ['true', 'false', 'null', '-Infinity', 'Infinity', 'NaN']:
        tokid = token.STRING
        tokval = u'"%s"' % tokval

    # fix single-quoted strings
    elif (tokid == token.STRING):
      if tokval.startswith ("'"):
        tokval = u'"%s"' % tokval[1:-1].replace ('"', '\\"')

    # remove invalid commas
    elif (tokid == token.OP) and ((tokval == '}') or (tokval == ']')):
      if (len(result) > 0) and (result[-1][1] == ','):
        result.pop()

    # detect single-line comments
    elif tokval == "//":
      sline_comment = True
      continue

    # detect multiline comments
    elif (last_token == '/') and (tokval == '*'):
      result.pop() # remove previous token
      mline_comment = True
      continue

    result.append((tokid, tokval))
    last_token = tokval

  return tokenize.untokenize(result)
