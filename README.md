# common-golang

Contrats Go partagés par les applications Suppa Stack. Le module cible Go
1.27.0 et centralise notamment :

- le client et les DTO de l’API interne `auth_app` (`authapp`) ;
- le cycle de vie local des sessions, rôles, rotations et CSRF (`authsession`) ;
- le runtime SSE et les helpers HTTP/CSP/i18n (`sse`, `serverutil`) ;
- les notifications et composants Templ réutilisables (`notifications`, `ui`).

Les applications consommatrices conservent leurs règles de rôles et leurs
handlers métier ; elles ne doivent pas recopier les contrats réseau ni la
gestion générique des sessions.

```sh
templ generate
go test ./...
```

`dbready` vérifie avec le pool applicatif les droits nécessaires au runtime et
expose un handler `/ready` (200/503), sans diagnostics SQL publics. `dbschema`
adapte les migrations embarquées à un namespace configuré, en citant les
identifiants numériques et en remappant les rôles de tâche injectés. Les erreurs
techniques de validation de session produisent un 503 ; une navigation anonyme
est redirigée vers auth-app, tandis qu'une requête JSON reçoit un 401.
