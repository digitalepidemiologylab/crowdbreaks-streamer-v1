ERROR_CODES = {
        304: {'text': 'Not Modified', 'description': 'There was no new data to return.'},
        400: {'text': 'Bad Request.', 'description': 'The request was invalid or cannot be otherwise served. An accompanying error message will explain further. Requests without authentication are considered invalid and will yield this response.'},
        401: {'text': 'Unauthorized.', 'description': 'Missing or incorrect authentication credentials. This may also returned in other undefined circumstances.'},
        403: {'text': 'Forbidden', 'description': 'The request is understood, but it has been refused or access is not allowed. An accompanying error message will explain why. This code is used when requests are being denied due to update limits . Other reasons for this status being returned are listed alongside the error codes in the table below.'},
        404: {'text': 'Not Found', 'description': ''},
        406: {'text': 'Not Acceptable', 'description': 'Returned when an invalid format is specified in the request.'},
        410: {'text': 'Gone', 'description': 'This resource is gone. Used to indicate that an API endpoint has been turned off.'},
        420: {'text': 'Enhance your Calm.', 'description': 'Returned when an app is being rate limited for making too many requests.'},
        422: {'text': 'Unprocessable Entity', 'description': 'Returned when the data is unable to be processed (for example, if an image uploaded to POST account / update_profile_banner is not valid, or the JSON body of a request is badly-formed).'},
        429: {'text': 'Too Many Requests', 'description': "Returned when a request cannot be served due to the app's rate limit having been exhausted for the resource. See Rate Limiting."},
        500: {'text': 'Internal Server Error', 'description': 'Something is broken. This is usually a temporary error, for example in a high load situation or if an endpoint is temporarily having issues. Check in the developer forums in case others are having similar issues,  or try again later.'},
        502: {'text': 'Bad Gateway', 'description': 'Twitter is down, or being upgraded.'},
        503: {'text': 'Service Unavailable', 'description': 'The Twitter servers are up, but overloaded with requests. Try again later.'},
        504: {'text': 'Gateway timeout', 'description': 'The Twitter servers are up, but the request couldn’t be serviced due to some failure within the internal stack. Try again later.'}
        }
