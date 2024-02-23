import Link from 'next/link';


export default function TopNav() {
    return (
        <div className="flex space-x-4 mx-auto my-10 bg-gray-300 rounded-full w-48 h-8 justify-center content-center">
            <Link className="my-auto " href="">
                <p className="text-black">Realtime</p>
            </Link>
            <Link className="my-auto" href="">
                <p className="text-black">Histrorical</p>
            </Link>
        </div>

    )
}