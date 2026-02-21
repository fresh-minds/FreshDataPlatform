import { useContext } from 'react';
import { AuthContext } from './KeycloakProvider';

export default function useAuth() {
    return useContext(AuthContext);
}
