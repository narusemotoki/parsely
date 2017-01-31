import falcon

import denqueue

application = falcon.API()
application.add_route("/enqueue/{queue_name}", denqueue.Enqueue())
