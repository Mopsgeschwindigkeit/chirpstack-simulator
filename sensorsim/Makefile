deploy-sx:
	docker build . --tag smaxtec/sx-sensorsim:latest
	docker push smaxtec/sx-sensorsim

simstats:
	kubectl -n kafka cp simstats.py kafka-maintain-service:simstats.py && kubectl exec -it -n kafka kafka-maintain-service -- python simstats.py
