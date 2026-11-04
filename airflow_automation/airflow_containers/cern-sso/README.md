# CERN SSO for the one-pod deployment

A layer on [the one-pod deployment](../README.md#one-pod-on-kubernetes), for any cluster that can reach CERN's SSO:
logging in goes through a CERN account instead of the generated password, and who's an Admin is managed in CERN's
[Application Portal](https://application-portal.web.cern.ch), by e-group.

| | Without this layer | With it |
|---|---|---|
| Reading the UI | Anyone who can reach it, no login | The same |
| Logging in, at `/auth/login/` | `admin`, with the generated password | A CERN account, through "Sign In with cern" |
| Being an Admin | Whoever has that password | Whoever the portal gives the `admin` role |
| Anyone else who logs in | | A Viewer |

[`kustomization.yaml`](kustomization.yaml) is a kustomize Component, to add to whichever overlay deploys the pod. It has
FAB read [`webserver_config.py`](webserver_config.py), which is in the image, and takes the registration's details from a
Secret.

> **Status:** run in a browser against a Keycloak set up the way CERN's documentation describes its SSO
> (`tests/airflow-ngt/test_airflow_cern_sso_live.py`), over plain HTTP and behind a TLS-terminating proxy, but **not yet
> against CERN's SSO itself**. If something differs in reality, trust
> [its documentation](https://auth.docs.cern.ch) and please correct this page.

## 1. Register the application

In the Application Portal, register an application with these, or use one you have:

- **Return URI:** `https://<the UI's address>/auth/oauth-authorized/cern`.
- **An OIDC client**, whose ID and secret go into a Secret next to the pod:

```bash
kubectl create secret generic oidc-client-secret \
  --from-literal=clientID=<client ID> \
  --from-literal=clientSecret=<client secret> \
  --from-literal=issuerURL=https://auth.cern.ch/auth/realms/cern
```

On CERN's OpenShift every project has a registration and that Secret already: see
[its tutorial](../openshift/README.md).

## 2. Say who's an Admin

In the Application Portal, under your registration -> Roles, add a role with the identifier `admin`, and add the e-groups
that should have it. To use another identifier, set `NGT_SSO_ADMIN_ROLE` on the `airflow` container. It takes effect at
each login, so someone who loses the role stops being an Admin the next time they log in.

## 3. Add the layer to your overlay

```yaml
# my-cluster/kustomization.yaml, next to onepod/ and cern-sso/. Then: kubectl apply -k my-cluster/
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../onepod
components:
  - ../cern-sso
```

The UI needs an `https://` address, and the pod has to reach `auth.cern.ch`. Set `AIRFLOW__FAB__COOKIE_SECURE` to `"True"`
too, as [for any login behind TLS](../README.md#3-open-the-ui).

## Worth knowing

- **To have people log in for reading as well**, set `AIRFLOW__FAB__AUTH_ROLE_PUBLIC` to `""`. Then whoever the
  registration lets in is a Viewer once logged in, and nobody else sees anything. Who it lets in is the registration's default
  role: by default all CERN and eduGAIN users.
- **The generated password stays**, for the API only: `POST /auth/token` still takes `admin` with it, and the login page
  doesn't. That's a way back in if the SSO is misconfigured, for whoever can read it from inside the pod.
- **FAB's users are in the pod's database**, like everything else, so a new pod starts with none, and people are added back
  as they log in.

## If something goes wrong

| What you see | Likely cause | What to do |
|---|---|---|
| `CreateContainerConfigError` on `airflow` | No Secret called `oidc-client-secret` | Step 1 |
| The SSO says `Invalid parameter: redirect_uri` | The return URI isn't registered, or isn't the address it asks for | Compare it with the `redirect_uri` in the address bar. `http://` there means the proxy doesn't send `X-Forwarded-Proto` |
| You log in and are only a Viewer | No `admin` role in the portal for you, or another identifier | Step 2, then log in again |
| You come back to the login page | The session cookie didn't survive the trip | `AIRFLOW__FAB__COOKIE_SECURE` only works on `https://`: is the address you're on one? |
